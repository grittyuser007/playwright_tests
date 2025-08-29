import asyncio
import json
import os
import time
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime

from dotenv import load_dotenv
from playwright.async_api import async_playwright, Page, TimeoutError as PlaywrightTimeoutError


# Configuration
ROOT = Path(__file__).resolve().parent
STORAGE_FILE = ROOT / "storage_state.json"
OUTPUT_FILE = ROOT / "products.json"
ENV_PATH = ROOT / ".env"
SCREENSHOTS_DIR = ROOT / "debug_screenshots"

# Create screenshots directory if it doesn't exist
SCREENSHOTS_DIR.mkdir(exist_ok=True)

# Load environment variables from .env (if present)
load_dotenv(dotenv_path=str(ENV_PATH))
EMAIL = os.getenv("EMAIL")
PASSWORD = os.getenv("PASSWORD")
HEADLESS = os.getenv("HEADLESS", "true").lower() in ("1", "true", "yes")
TARGET_URL = os.getenv("TARGET_URL", "https://hiring.idenhq.com/")


async def take_debug_screenshot(page: Page, step_name: str) -> None:
    """Take a screenshot for debugging purposes."""
    timestamp = datetime.now().strftime("%H%M%S")
    filename = f"{step_name}_{timestamp}.png"
    screenshot_path = SCREENSHOTS_DIR / filename
    try:
        await page.screenshot(path=str(screenshot_path), full_page=True)
        print(f"Debug screenshot saved: {screenshot_path}")
    except Exception as e:
        print(f"Failed to take screenshot {filename}: {e}")


async def check_and_recover_session(page: Page) -> bool:
    """Check if we're still logged in and recover if needed."""
    current_url = page.url
    
    # Check if we're on login page or logged out
    if "login" in current_url.lower() or await page.query_selector("input[type=\"email\"]"):
        print("Session lost, attempting to re-login...")
        await take_debug_screenshot(page, "session_lost")
        
        # Try to login again
        success = await try_login(page)
        if success:
            print("Re-login successful")
            # Navigate back to challenge
            await page.goto(TARGET_URL.rstrip("/") + "/challenge", wait_until="networkidle")
            await page.wait_for_timeout(2000)
            return True
        else:
            print("Re-login failed")
            return False
    
    return True


async def debug_page_content(page: Page, step_name: str) -> None:
    """Print page content and structure for debugging."""
    print(f"\n=== DEBUG: {step_name} ===")
    try:
        # Get page title and URL
        title = await page.title()
        url = page.url
        print(f"Page Title: {title}")
        print(f"Page URL: {url}")
        
        # Get visible text content (first 500 chars)
        body_text = await page.evaluate("() => document.body.innerText")
        if body_text:
            print(f"Page Content (first 500 chars): {body_text[:500]}...")
        
        # Look for common table/grid elements
        table_elements = await page.query_selector_all("table, [role=table], .ag-grid, .react-table, .data-table")
        print(f"Found {len(table_elements)} table-like elements")
        
        # Look for buttons
        buttons = await page.query_selector_all("button")
        button_texts = []
        for btn in buttons[:10]:  # Limit to first 10 buttons
            try:
                text = await btn.inner_text()
                if text.strip():
                    button_texts.append(text.strip())
            except:
                pass
        print(f"Visible buttons: {button_texts}")
        
    except Exception as e:
        print(f"Debug failed for {step_name}: {e}")
    print("=== END DEBUG ===\n")


async def is_logged_in(page: Page) -> bool:
    # Heuristics: presence of "Launch Challenge" or absence of common sign-in elements
    try:
        # Look for an element that appears only when authenticated
        if await page.query_selector("text=Launch Challenge"):
            return True
        # If there's a "Sign in" button or email input, consider user logged out
        if await page.query_selector("input[type=\"email\"]") or await page.query_selector("text=Sign in") or await page.query_selector("text=Sign In"):
            return False
        # Fallback: inspect for a profile or logout
        if await page.query_selector("text=Logout") or await page.query_selector("text=Sign out"):
            return True
    except PlaywrightTimeoutError:
        pass
    # Default to False to be safe
    return False


async def try_login(page: Page) -> bool:
    print("Attempting login using credentials from .env...")
    if not EMAIL or not PASSWORD:
        print("EMAIL or PASSWORD not found in environment. Aborting login.")
        return False

    # Try to open a login form in several ways
    # 1) Click obvious sign-in links
    for sel in ["text=Sign in", "text=Sign In", "text=Login", "text=Log in"]:
        try:
            btn = await page.query_selector(sel)
            if btn:
                await btn.click()
                await page.wait_for_timeout(500)
                break
        except Exception:
            pass

    # 2) If an email input exists, fill it
    try:
        email_input = await page.wait_for_selector("input[type=\"email\"]", timeout=2000)
        await email_input.fill(EMAIL)
        pwd = await page.query_selector("input[type=\"password\"]")
        if not pwd:
            # try generic password input
            pwd = await page.query_selector("input[name=\"password\"]")
        if pwd:
            await pwd.fill(PASSWORD)
        # Click submit if available
        for submit_sel in ["button[type=submit]", "text=Sign in", "text=Sign In", "text=Login", "text=Log in"]:
            try:
                submit = await page.query_selector(submit_sel)
                if submit:
                    await submit.click()
                    break
            except Exception:
                pass

        # wait for navigation or login effect
        await page.wait_for_timeout(2000)
    except PlaywrightTimeoutError:
        # no email input found - try alternative: navigate to /login
        try:
            await page.goto(TARGET_URL.rstrip("/") + "/login", wait_until="networkidle")
            await page.wait_for_timeout(1000)
            if await page.query_selector("input[type=\"email\"]"):
                await page.fill("input[type=\"email\"]", EMAIL)
                if await page.query_selector("input[type=\"password\"]"):
                    await page.fill("input[type=\"password\"]", PASSWORD)
                if await page.query_selector("button[type=submit]"):
                    await page.click("button[type=submit]")
                await page.wait_for_timeout(2000)
        except Exception:
            pass

    # Small wait and then check
    try:
        await page.wait_for_load_state("networkidle", timeout=5000)
    except Exception:
        pass
    logged_in = await is_logged_in(page)
    print(f"Login result: {logged_in}")
    return logged_in


async def click_text_button(page: Page, texts: List[str]) -> bool:
    """Try clicking the first button matching any of the texts (case-insensitive)."""
    for t in texts:
        # Try multiple selector strategies
        selectors = [
            f"text=/{t}/i",
            f"button:has-text(\"{t}\")",
            f"[role=button]:has-text(\"{t}\")",
            f"button >> text={t}",
        ]
        
        for sel in selectors:
            try:
                btn = await page.query_selector(sel)
                if btn:
                    # Check if button is visible and enabled
                    is_visible = await btn.is_visible()
                    is_enabled = await btn.is_enabled()
                    
                    if is_visible and is_enabled:
                        print(f"Clicking button with text: {t}")
                        await btn.click()
                        await page.wait_for_timeout(800)
                        return True
            except Exception as e:
                continue
    
    print(f"Could not find clickable button with any of these texts: {texts}")
    return False


async def click_first_option_then_next(page: Page, option_texts: Optional[List[str]] = None) -> bool:
    # Try to click a specific option, otherwise fallback to clicking the first enabled option (not Next/Back)
    # Option selectors
    clicked = False
    if option_texts:
        clicked = await click_text_button(page, option_texts)
    if not clicked:
        # find first candidate button inside a modal/step that is not Next/Back
        try:
            buttons = await page.query_selector_all("button:visible")
            for b in buttons:
                txt = (await b.inner_text()).strip().lower()
                if not txt:
                    continue
                if any(x in txt for x in ("next", "back", "cancel", "skip", "close", "sign out", "sign in")):
                    continue
                # click it
                try:
                    await b.click()
                    clicked = True
                    await page.wait_for_timeout(600)
                    print(f"Clicked option button: {txt}")
                    break
                except Exception:
                    continue
        except Exception:
            pass

    # Wait a bit for any animations/transitions
    await page.wait_for_timeout(1000)
    
    # Click Next (if present)
    next_clicked = await click_text_button(page, ["Next", "next", "Continue", "continue", "Proceed", "View Products"])
    
    # Wait for navigation/loading
    await page.wait_for_timeout(1500)
    
    return clicked or next_clicked 


async def find_table_container(page: Page) -> Optional[str]:
    # Try common selectors for tables or scrollable containers
    candidates = [
        "table",
        "table:visible",
        "[role=table]",
        "[role=grid]",
        ".product-table",
        ".table",
        ".ag-center-cols-container",  # ag-grid
        ".ag-body-container",
        ".ag-body-viewport", 
        ".react-table",
        ".data-table",
        "div[style*='overflow']",
        ".table-container",
        ".grid-container",
        ".products-container",
        ".list-container"
    ]
    
    print(f"Checking {len(candidates)} potential table container selectors...")
    
    for sel in candidates:
        try:
            el = await page.query_selector(sel)
            if el:
                # Check if element is visible and has content
                is_visible = await el.is_visible()
                if is_visible:
                    print(f"Found visible table container: {sel}")
                    return sel
                else:
                    print(f"Found but not visible: {sel}")
        except Exception as e:
            print(f"Error checking selector {sel}: {e}")
    
    # Fallback: try to find a scrollable div with content
    print("Trying fallback: looking for scrollable divs...")
    try:
        divs = await page.query_selector_all("div")
        print(f"Checking {len(divs)} div elements for scrollable content...")
        
        for i, d in enumerate(divs):
            try:
                # Check if div has overflow styling
                style = await d.get_attribute("style") or ""
                class_name = await d.get_attribute("class") or ""
                
                # Check for scrollable indicators
                is_scrollable = (
                    "overflow" in style and ("auto" in style or "scroll" in style) or
                    "scroll" in class_name.lower() or
                    "table" in class_name.lower() or
                    "grid" in class_name.lower() or
                    "list" in class_name.lower()
                )
                
                if is_scrollable:
                    # Check if it has text content
                    text_content = await d.inner_text()
                    if text_content and len(text_content.strip()) > 50:  # Has substantial content
                        print(f"Found scrollable div with content (index {i}): class='{class_name}', style='{style[:100]}...'")
                        return f"div:nth-of-type({i+1})"
                        
            except Exception as e:
                continue
                
    except Exception as e:
        print(f"Error in fallback search: {e}")
    
    return None


async def extract_rows_from_container(page: Page, container_sel: str, seen: set) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    # Attempt multiple row selectors
    row_selectors = ["tr", ".ag-row", "div[role=\"row\"]", "tbody tr"]
    
    for rs in row_selectors:
        try:
            locator = f"{container_sel} {rs}"
            elements = await page.query_selector_all(locator)
            if not elements:
                continue
                
            print(f"Found {len(elements)} rows with selector: {locator}")
            
            for el in elements:
                try:
                    # Check if element is visible
                    is_visible = await el.is_visible()
                    if not is_visible:
                        continue
                        
                    text = (await el.inner_text()).strip()
                    if not text:
                        continue
                    if text in seen:
                        continue
                    seen.add(text)
                    
                    # split columns by tabs/newlines as fallback
                    cells = [c.strip() for c in text.split("\t") if c.strip()]
                    if not cells:  # Try splitting by multiple spaces
                        cells = [c.strip() for c in text.split("  ") if c.strip()]
                    
                    rows.append({"raw": text, "cells": cells})
                except Exception as e:
                    continue
            if rows:
                return rows
        except Exception as e:
            continue
    return rows


async def scroll_container_and_collect(page: Page, container_sel: str) -> List[Dict[str, Any]]:
    seen = set()
    all_rows: List[Dict[str, Any]] = []
    
    # Extract all currently visible rows without scrolling
    new_rows = await extract_rows_from_container(page, container_sel, seen)
    if new_rows:
        all_rows.extend(new_rows)
    
    return all_rows


async def try_click_next_pagination(page: Page) -> bool:
    # Try to find pagination 'Next' outside of table
    pagination_selectors = [
        "text=Next",
        "text=next", 
        "button[aria-label=\"Next\"]",
        "button[aria-label=\"next\"]",
        "button:has-text('Next')",
        "button:has-text('>')",
        "[role=button]:has-text('Next')",
        "[role=button]:has-text('>')",
        ".pagination button:last-child",
        ".pager button:last-child",
        "button[class*='next']",
        "a[class*='next']"
    ]
    
    for sel in pagination_selectors:
        try:
            btn = await page.query_selector(sel)
            if btn and await btn.is_visible():
                disabled = await btn.get_attribute("disabled")
                aria_disabled = await btn.get_attribute("aria-disabled")
                
                if disabled == "true" or aria_disabled == "true":
                    print(f"Pagination button {sel} is disabled")
                    return False
                    
                print(f"Clicking pagination button: {sel}")
                await btn.click()
                await page.wait_for_timeout(2000)
                return True
        except Exception as e:
            continue
    
    print("No pagination buttons found")
    return False


async def main() -> None:
    print("Starting scraper")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS)
        context = None
        page = None
        try:
            # If we have a storage file, try to reuse it
            if STORAGE_FILE.exists():
                print(f"Found existing storage state at {STORAGE_FILE}, reusing session")
                context = await browser.new_context(storage_state=str(STORAGE_FILE))
                page = await context.new_page()
                await page.goto(TARGET_URL, wait_until="networkidle")
                if not await is_logged_in(page):
                    # try login fresh using a new context
                    print("Existing storage didn't yield a logged-in session. Logging in interactively...")
                    await context.close()
                    context = await browser.new_context()
                    page = await context.new_page()
                    await page.goto(TARGET_URL, wait_until="networkidle")
                    ok = await try_login(page)
                    if ok:
                        await context.storage_state(path=str(STORAGE_FILE))
            else:
                context = await browser.new_context()
                page = await context.new_page()
                await page.goto(TARGET_URL, wait_until="networkidle")
                if not await is_logged_in(page):
                    ok = await try_login(page)
                    if ok:
                        await context.storage_state(path=str(STORAGE_FILE))

            # At this point we should be on the main page, logged in
            await take_debug_screenshot(page, "01_logged_in")
            await debug_page_content(page, "After Login")
            
            # Find and click Launch Challenge (or similarly labeled button)
            print("Navigating to challenge launcher...")
            await click_text_button(page, ["Launch Challenge", "Launch challenge", "Start Challenge", "Start"])
            await page.wait_for_timeout(1000)
            await take_debug_screenshot(page, "02_after_launch")

            # Step 1: Select Data Source -> prefer Local Database
            print("Selecting data source...")
            await debug_page_content(page, "Step 1 - Before Data Source Selection")
            
            if not await check_and_recover_session(page):
                print("Session recovery failed at step 1")
                return
                
            success = await click_first_option_then_next(page, option_texts=["Local Database", "Local DB", "Local database"])
            await take_debug_screenshot(page, "03_after_data_source")
            
            if not success:
                print("Failed to complete step 1")
                return

            # Step 2: Choose Category
            print("Choosing a category...")
            await debug_page_content(page, "Step 2 - Before Category Selection")
            
            if not await check_and_recover_session(page):
                print("Session recovery failed at step 2")
                return
                
            success = await click_first_option_then_next(page)
            await take_debug_screenshot(page, "04_after_category")
            
            if not success:
                print("Failed to complete step 2")
                return

            # Step 3: Select View Type
            print("Selecting view type...")
            await debug_page_content(page, "Step 3 - Before View Type Selection")
            
            if not await check_and_recover_session(page):
                print("Session recovery failed at step 3")
                return
                
            success = await click_first_option_then_next(page)
            await take_debug_screenshot(page, "05_after_view_type")
            
            if not success:
                print("Failed to complete step 3")
                return

            # Step 4: View Products / Finish
            print("Finalizing and opening products view...")
            await debug_page_content(page, "Step 4 - Before Final Step")
            
            if not await check_and_recover_session(page):
                print("Session recovery failed at step 4")
                return
            
            # First complete step 3 properly
            success = await click_first_option_then_next(page)
            if not success:
                print("Failed to complete step 3")
                return
                
            await take_debug_screenshot(page, "06_after_step3")
            await debug_page_content(page, "After Step 3 Completion")
            
            # Now try to trigger products view
            print("Attempting to load products view...")
            view_clicked = await click_text_button(page, ["View Products", "View products", "View Product", "Finish", "Open"])
            
            if not view_clicked:
                print("Trying alternative methods to load products...")
                # Try pressing Enter
                await page.keyboard.press("Enter")
                await page.wait_for_timeout(1000)
                
                # Try clicking any submit-like buttons
                submit_buttons = await page.query_selector_all("button[type=submit], input[type=submit]")
                for btn in submit_buttons:
                    try:
                        if await btn.is_visible():
                            await btn.click()
                            break
                    except:
                        continue
            
            # Wait for products to load with timeout
            print("Waiting for products to load...")
            
            # Try waiting for table elements to appear with shorter timeout
            try:
                await page.wait_for_selector("table, [role=table], .ag-grid, tbody tr", timeout=5000)
                print("Table elements detected!")
            except:
                print("No table elements appeared after 5 seconds")
            
            await take_debug_screenshot(page, "07_final_products_page")
            
            # Inspect table structure for virtual scrolling
            table_info = await page.evaluate("""
                () => {
                    const table = document.querySelector('table');
                    if (!table) return null;
                    
                    const info = {
                        tableClasses: table.className,
                        tableStyle: table.style.cssText,
                        parentClasses: table.parentElement ? table.parentElement.className : null,
                        scrollHeight: table.scrollHeight,
                        clientHeight: table.clientHeight,
                        scrollTop: table.scrollTop
                    };
                    
                    // Look for virtual scrolling indicators
                    const virtualScrollIndicators = [
                        'ag-grid', 'react-table', 'virtual', 'virtualized'
                    ];
                    
                    info.isVirtualScroll = virtualScrollIndicators.some(indicator => 
                        table.className.toLowerCase().includes(indicator) ||
                        (table.parentElement && table.parentElement.className.toLowerCase().includes(indicator))
                    );
                    
                    return info;
                }
            """)
            
            print(f"Table structure info: {table_info}")

            # Wait for product table or container to appear
            print("Looking for product table or scrollable product area...")
            await debug_page_content(page, "Looking for Table Container")
            
            container_sel = await find_table_container(page)
            if not container_sel:
                print("Could not find a table container by simple heuristics. Trying to wait for any table-like structure...")
                try:
                    await page.wait_for_selector("table, tbody tr, .ag-row", timeout=5000)
                    container_sel = await find_table_container(page)
                except Exception as e:
                    print(f"Exception while waiting for table: {e}")

            if not container_sel:
                print("ERROR: No table container found. Taking final debug screenshot...")
                await take_debug_screenshot(page, "07_ERROR_no_table")
                await debug_page_content(page, "ERROR - No Table Found")
                
                # Try to find ANY scrollable or data container
                print("Attempting to find any data containers...")
                all_divs = await page.query_selector_all("div")
                print(f"Found {len(all_divs)} div elements on page")
                
                # Check for common data display patterns
                data_patterns = [
                    "div[class*='table']",
                    "div[class*='grid']", 
                    "div[class*='list']",
                    "div[class*='data']",
                    "div[class*='product']",
                    "div[class*='row']",
                    "div[class*='item']"
                ]
                
                for pattern in data_patterns:
                    elements = await page.query_selector_all(pattern)
                    if elements:
                        print(f"Found {len(elements)} elements matching pattern: {pattern}")
                        
                # Try to inspect the page HTML structure
                print("Inspecting page HTML for hidden content...")
                try:
                    # Get all elements with display:none or visibility:hidden
                    hidden_elements = await page.evaluate("""
                        () => {
                            const elements = document.querySelectorAll('*');
                            const hidden = [];
                            elements.forEach(el => {
                                const style = window.getComputedStyle(el);
                                if (style.display === 'none' || style.visibility === 'hidden') {
                                    if (el.textContent && el.textContent.trim().length > 20) {
                                        hidden.push({
                                            tag: el.tagName,
                                            class: el.className,
                                            text: el.textContent.substring(0, 100)
                                        });
                                    }
                                }
                            });
                            return hidden.slice(0, 5); // Limit to first 5
                        }
                    """)
                    
                    if hidden_elements:
                        print("Found hidden elements with content:")
                        for el in hidden_elements:
                            print(f"  {el['tag']}.{el['class']}: {el['text']}...")
                            
                    # Check for iframes
                    iframes = await page.query_selector_all("iframe")
                    if iframes:
                        print(f"Found {len(iframes)} iframes on page")
                        
                except Exception as e:
                    print(f"Error inspecting HTML: {e}")
                
                (OUTPUT_FILE).write_text(json.dumps([], indent=2))
                return

            print(f"Using container selector: {container_sel}")
            
            # First collect initial data
            products = await scroll_container_and_collect(page, container_sel)
            print(f"Initial collection: {len(products)} products")
            
            # Check if there are more products to load (showing X of Y pattern)
            page_text = await page.evaluate("() => document.body.innerText")
            if "showing" in page_text.lower() and "of" in page_text.lower():
                import re
                match = re.search(r'showing\s+(\d+)\s+of\s+(\d+)', page_text.lower())
                if match:
                    current_count = int(match.group(1))
                    total_count = int(match.group(2))
                    print(f"Found pagination info: showing {current_count} of {total_count} products")
                    
                    if total_count > current_count:
                        print("Attempting to load more products via scrolling and pagination...")
                        
                        # Virtual scrolling strategy - scroll within the table container only
                        print("Attempting virtual scrolling within table container...")
                        max_scroll_attempts = 100
                        
                        for scroll_attempt in range(max_scroll_attempts):
                            try:
                                # Advanced virtual scrolling - try multiple container elements
                                scrolled = await page.evaluate(f"""
                                    (sel) => {{
                                        // Try to find the actual scrollable container
                                        let scrollableEl = null;
                                        
                                        // First try the table itself
                                        const table = document.querySelector(sel);
                                        if (table) {{
                                            // Look for parent containers that might be scrollable
                                            let parent = table.parentElement;
                                            while (parent && parent !== document.body) {{
                                                const style = window.getComputedStyle(parent);
                                                if (style.overflow === 'auto' || style.overflow === 'scroll' || 
                                                    style.overflowY === 'auto' || style.overflowY === 'scroll') {{
                                                    scrollableEl = parent;
                                                    break;
                                                }}
                                                parent = parent.parentElement;
                                            }}
                                            
                                            // If no scrollable parent found, try the table itself
                                            if (!scrollableEl) {{
                                                scrollableEl = table;
                                            }}
                                        }}
                                        
                                        if (!scrollableEl) return false;
                                        
                                        const prevScrollTop = scrollableEl.scrollTop;
                                        const scrollHeight = scrollableEl.scrollHeight;
                                        const clientHeight = scrollableEl.clientHeight;
                                        
                                        // Scroll by a reasonable amount
                                        const scrollAmount = Math.min(clientHeight * 0.8, 500);
                                        scrollableEl.scrollTop = prevScrollTop + scrollAmount;
                                        
                                        // Return true if we actually scrolled
                                        return scrollableEl.scrollTop > prevScrollTop;
                                    }}
                                """, container_sel)
                                
                                if scrolled:
                                    await page.wait_for_timeout(1500)  # Wait for virtual scroll to load content
                                    new_products = await scroll_container_and_collect(page, container_sel)
                                    if len(new_products) > len(products):
                                        print(f"Virtual scroll {scroll_attempt + 1}: Found {len(new_products)} total products")
                                        products = new_products
                                    else:
                                        print(f"Virtual scroll {scroll_attempt + 1}: No new products loaded")
                                        break
                                else:
                                    print(f"Virtual scroll {scroll_attempt + 1}: Cannot scroll further")
                                    break
                            except Exception as e:
                                print(f"Virtual scroll error: {e}")
                                break
                        
                        # Strategy 3: Infinite scroll with loading dots detection
                        print("Using infinite scroll to collect ALL products...")
                        print(f"Target: {total_count} products")
                        
                        max_scroll_attempts = 500  # Increase for all products
                        consecutive_no_change = 0
                        start_time = time.time()
                        max_time_minutes = 45  # Maximum 45 minutes for safety
                        
                        for scroll_attempt in range(max_scroll_attempts):
                            try:
                                # Check timeout
                                elapsed_time = time.time() - start_time
                                if elapsed_time > (max_time_minutes * 60):
                                    print(f"Timeout reached ({max_time_minutes} minutes), stopping")
                                    break
                                
                                # Try multiple scrolling approaches for virtual scrolling tables
                                scroll_result = await page.evaluate(f"""
                                    () => {{
                                        const table = document.querySelector('{container_sel}');
                                        const rows = document.querySelectorAll('{container_sel} tr');
                                        
                                        // Find scrollable parent containers
                                        let scrollableParent = null;
                                        let current = table;
                                        while (current && current !== document.body) {{
                                            const style = window.getComputedStyle(current);
                                            if (style.overflow === 'auto' || style.overflow === 'scroll' || 
                                                style.overflowY === 'auto' || style.overflowY === 'scroll') {{
                                                scrollableParent = current;
                                                break;
                                            }}
                                            current = current.parentElement;
                                        }}
                                        
                                        let scrolled = false;
                                        
                                        // Strategy 1: Scroll the scrollable parent
                                        if (scrollableParent) {{
                                            const prevScroll = scrollableParent.scrollTop;
                                            scrollableParent.scrollTop = scrollableParent.scrollHeight;
                                            scrolled = scrollableParent.scrollTop > prevScroll;
                                        }}
                                        
                                        // Strategy 2: Scroll table itself
                                        if (table && !scrolled) {{
                                            const prevScroll = table.scrollTop;
                                            table.scrollTop = table.scrollHeight;
                                            scrolled = table.scrollTop > prevScroll;
                                        }}
                                        
                                        // Strategy 3: Scroll to last row
                                        if (rows.length > 0) {{
                                            const lastRow = rows[rows.length - 1];
                                            lastRow.scrollIntoView({{ behavior: 'auto', block: 'end' }});
                                        }}
                                        
                                        // Strategy 4: Scroll page
                                        const prevPageScroll = window.pageYOffset;
                                        window.scrollTo(0, document.body.scrollHeight);
                                        const pageScrolled = window.pageYOffset > prevPageScroll;
                                        
                                        return {{
                                            scrolled: scrolled || pageScrolled,
                                            rowCount: rows.length,
                                            scrollableParent: scrollableParent ? scrollableParent.tagName + '.' + scrollableParent.className : null
                                        }};
                                    }}
                                """)
                                
                                print(f"Scroll result: {scroll_result}")
                                
                                # Try mouse wheel scrolling as well
                                try:
                                    # Get the table element and simulate mouse wheel
                                    table_element = await page.query_selector(container_sel)
                                    if table_element:
                                        # Scroll down with mouse wheel
                                        await table_element.hover()
                                        await page.mouse.wheel(0, 1000)  # Scroll down 1000 pixels
                                        await page.wait_for_timeout(500)
                                        
                                        # Try multiple wheel scrolls
                                        for _ in range(3):
                                            await page.mouse.wheel(0, 500)
                                            await page.wait_for_timeout(200)
                                except Exception as e:
                                    print(f"Mouse wheel scroll failed: {e}")
                                
                                # Wait a bit for scroll to register
                                await page.wait_for_timeout(500)
                                
                                # Look for loading indicators (dots, spinners, etc.)
                                loading_indicators = [
                                    "text=Loading",
                                    "text=...",
                                    "text=•••",
                                    ".loading",
                                    ".spinner",
                                    ".dots",
                                    "[class*='loading']",
                                    "[class*='spinner']",
                                    "[class*='dots']",
                                    "[aria-label*='loading']",
                                    "div:has-text('...')",
                                    "span:has-text('...')"
                                ]
                                
                                # Check for loading indicators and wait
                                loading_detected = False
                                loading_element = None
                                
                                for indicator in loading_indicators:
                                    try:
                                        element = await page.query_selector(indicator)
                                        if element and await element.is_visible():
                                            loading_detected = True
                                            loading_element = element
                                            print(f"Loading detected: {indicator}")
                                            break
                                    except:
                                        continue
                                
                                # Always wait a bit for potential loading to start
                                await page.wait_for_timeout(1000)
                                
                                # Check again for loading indicators that might have appeared
                                if not loading_detected:
                                    for indicator in loading_indicators:
                                        try:
                                            element = await page.query_selector(indicator)
                                            if element and await element.is_visible():
                                                loading_detected = True
                                                loading_element = element
                                                print(f"Loading detected after wait: {indicator}")
                                                break
                                        except:
                                            continue
                                
                                # If loading detected, wait for it to finish
                                if loading_detected:
                                    print("Waiting for loading to complete...")
                                    try:
                                        # Wait for the specific loading element to disappear
                                        await page.wait_for_function(
                                            f"() => !document.querySelector('{loading_indicators[0]}') || !document.querySelector('{loading_indicators[0]}').offsetParent",
                                            timeout=15000
                                        )
                                    except:
                                        pass
                                    
                                    # Additional wait for content to render
                                    await page.wait_for_timeout(2000)
                                else:
                                    # No loading detected, but still wait for potential content
                                    await page.wait_for_timeout(2000)
                                
                                # Check if new rows have been added to the DOM
                                current_row_count = await page.evaluate(f"""
                                    () => {{
                                        const rows = document.querySelectorAll('{container_sel} tr');
                                        return rows.length;
                                    }}
                                """)
                                
                                # Collect products after loading
                                new_products = await scroll_container_and_collect(page, container_sel)
                                
                                if len(new_products) > len(products):
                                    print(f"Scroll {scroll_attempt + 1}: Progress {len(new_products)}/{total_count} products ({(len(new_products)/total_count)*100:.1f}%) - DOM rows: {current_row_count}")
                                    products = new_products
                                    consecutive_no_change = 0
                                    
                                    # Check if we've loaded all products
                                    if len(new_products) >= total_count:
                                        print(f"SUCCESS: All {total_count} products loaded!")
                                        break
                                else:
                                    consecutive_no_change += 1
                                    print(f"Scroll {scroll_attempt + 1}: No new products (consecutive: {consecutive_no_change}) - DOM rows: {current_row_count}")
                                    
                                    # If no new products for 8 consecutive attempts, try more aggressive scrolling
                                    if consecutive_no_change == 5:
                                        print("Trying more aggressive scrolling...")
                                        # Try keyboard scrolling
                                        await page.keyboard.press("End")
                                        await page.wait_for_timeout(1000)
                                        await page.keyboard.press("PageDown")
                                        await page.wait_for_timeout(1000)
                                        
                                    elif consecutive_no_change >= 8:
                                        print("No new products after multiple scroll attempts, checking if we're at the end...")
                                        
                                        # Check current page content for "showing X of Y"
                                        page_text = await page.evaluate("() => document.body.innerText")
                                        import re
                                        match = re.search(r'showing\s+(\d+)\s+of\s+(\d+)', page_text.lower())
                                        if match:
                                            current_shown = int(match.group(1))
                                            total_available = int(match.group(2))
                                            print(f"Page shows: {current_shown} of {total_available}")
                                            
                                            if current_shown >= total_available:
                                                print("All available products are shown!")
                                                break
                                        
                                        # Check if we can scroll further
                                        can_scroll = await page.evaluate(f"""
                                            () => {{
                                                const table = document.querySelector('{container_sel}');
                                                if (table) {{
                                                    return table.scrollTop < (table.scrollHeight - table.clientHeight - 10);
                                                }}
                                                return window.pageYOffset < (document.body.scrollHeight - window.innerHeight - 10);
                                            }}
                                        """)
                                        
                                        if not can_scroll:
                                            print("Reached end of scrollable content")
                                            break
                                        else:
                                            print("Can still scroll, continuing...")
                                            consecutive_no_change = 0  # Reset counter
                                            
                            except Exception as e:
                                print(f"Error during scroll attempt {scroll_attempt + 1}: {e}")
                                consecutive_no_change += 1
                                if consecutive_no_change >= 5:
                                    break
                                continue
                        
                        print(f"Infinite scroll completed. Final count: {len(products)} products")

            # Try pagination if available
            pag_round = 0
            while await try_click_next_pagination(page) and pag_round < 50:
                pag_round += 1
                print(f"Pagination page {pag_round}: collecting data...")
                await page.wait_for_timeout(2000)  # Wait for page to load
                more = await scroll_container_and_collect(page, container_sel)
                
                # Add new products (avoid duplicates)
                new_products_added = 0
                for product in more:
                    if product not in products:
                        products.append(product)
                        new_products_added += 1
                        
                print(f"Added {new_products_added} new products from page {pag_round}")
                
                if new_products_added == 0:
                    print("No new products found, stopping pagination")
                    break

            # Save output with summary
            print(f"\n=== SCRAPING COMPLETE ===")
            print(f"Total products collected: {len(products)}")
            
            # Check if we got the header row
            if products and "ID" in str(products[0]):
                actual_products = len(products) - 1  # Subtract header row
                print(f"Actual product records: {actual_products}")
            else:
                actual_products = len(products)
            
            print(f"Target was: 2849 products")
            if actual_products >= 2849:
                print("✅ SUCCESS: All products collected!")
            else:
                completion_rate = (actual_products / 2849) * 100
                print(f"📊 Completion rate: {completion_rate:.1f}%")
            
            print(f"Writing {len(products)} records to {OUTPUT_FILE}")
            with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
                json.dump(products, f, indent=2, ensure_ascii=False)
            
            print(f"✅ Data saved to {OUTPUT_FILE}")
            print("=== END ===\n")

        finally:
            if context:
                await context.close()
            await browser.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Interrupted by user")
