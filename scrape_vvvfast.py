import asyncio
import json
import os
import re
from pathlib import Path
from typing import List, Dict, Any
from dotenv import load_dotenv
from playwright.async_api import async_playwright, Page

# Configuration
ROOT = Path(__file__).resolve().parent
STORAGE_FILE = ROOT / "storage_state.json"
OUTPUT_FILE = ROOT / "products.json"
ENV_PATH = ROOT / ".env"

load_dotenv(dotenv_path=str(ENV_PATH))
EMAIL = os.getenv("EMAIL")
PASSWORD = os.getenv("PASSWORD")
HEADLESS = os.getenv("HEADLESS", "true").lower() in ("1", "true", "yes")
TARGET_URL = os.getenv("TARGET_URL")


async def is_logged_in(page: Page) -> bool:
    """Check if user is logged in"""
    try:
        if await page.query_selector("text=Launch Challenge"):
            return True
        if await page.query_selector("input[type=\"email\"]") or await page.query_selector("text=Sign in"):
            return False
        if await page.query_selector("text=Logout"):
            return True
    except:
        pass
    return False


async def try_login(page: Page) -> bool:
    """Attempt login with credentials"""
    if not EMAIL or not PASSWORD:
        print("Missing credentials")
        return False

    # Try clicking sign-in button
    for sel in ["text=Sign in", "text=Login"]:
        try:
            btn = await page.query_selector(sel)
            if btn:
                await btn.click()
                await page.wait_for_timeout(500)
                break
        except:
            pass

    # Fill credentials
    try:
        email_input = await page.wait_for_selector("input[type=\"email\"]", timeout=3000)
        await email_input.fill(EMAIL)
        pwd = await page.query_selector("input[type=\"password\"]")
        if pwd:
            await pwd.fill(PASSWORD)

        # Submit
        for submit_sel in ["button[type=submit]", "text=Sign in", "text=Login"]:
            try:
                submit = await page.query_selector(submit_sel)
                if submit:
                    await submit.click()
                    break
            except:
                pass

        await page.wait_for_timeout(2000)
    except:
        # Try direct navigation to login
        try:
            await page.goto(TARGET_URL.rstrip("/") + "/login")
            await page.wait_for_timeout(1000)
            await page.fill("input[type=\"email\"]", EMAIL)
            await page.fill("input[type=\"password\"]", PASSWORD)
            await page.click("button[type=submit]")
            await page.wait_for_timeout(2000)
        except:
            pass

    return await is_logged_in(page)


async def click_button_by_text(page: Page, texts: List[str]) -> bool:
    """Click button matching any of the given texts"""
    for text in texts:
        selectors = [
            f"text=/{text}/i",
            f"button:has-text(\"{text}\")",
            f"[role=button]:has-text(\"{text}\")"
        ]

        for sel in selectors:
            try:
                btn = await page.query_selector(sel)
                if btn and await btn.is_visible() and await btn.is_enabled():
                    await btn.click()
                    await page.wait_for_timeout(500)
                    return True
            except:
                continue
    return False


async def complete_step(page: Page, option_texts: List[str] = None) -> bool:
    """Complete a step by clicking option then Next"""
    clicked = False

    if option_texts:
        clicked = await click_button_by_text(page, option_texts)

    if not clicked:
        try:
            buttons = await page.query_selector_all("button:visible")
            for btn in buttons:
                text = (await btn.inner_text()).strip().lower()
                if text and not any(x in text for x in ("next", "back", "cancel", "skip", "close", "sign")):
                    await btn.click()
                    clicked = True
                    await page.wait_for_timeout(400)
                    break
        except:
            pass

    next_clicked = await click_button_by_text(page, ["Next", "Continue", "View Products"])
    await page.wait_for_timeout(800)

    return clicked or next_clicked


async def find_table_container(page: Page) -> str:
    """
    Find the table element selector. We will dynamically find the real scrollable
    ancestor in JS when scrolling (virtualized table).
    """
    selectors = [
        "table",
        "[role=table]",
        "[role=grid]",
    ]
    for sel in selectors:
        try:
            el = await page.query_selector(sel)
            if el and await el.is_visible():
                return sel
        except:
            continue
    # fallback – most pages render a real <table>
    return "table"


async def extract_new_products(page: Page, table_sel: str, seen_ids: set) -> List[Dict[str, Any]]:
    """
    Extract only newly visible rows efficiently (single DOM roundtrip).
    Deduplicate by the first column (ID).
    """
    rows = await page.evaluate("""
        (sel) => {
            const table = document.querySelector(sel);
            if (!table) return [];
            const trs = Array.from(table.querySelectorAll('tbody tr'))
                .filter(r => r.offsetParent !== null);
            return trs.map(tr => {
                const tds = Array.from(tr.querySelectorAll('td'));
                return tds.map(td => td.innerText.trim());
            });
        }
    """, table_sel)

    new_items: List[Dict[str, Any]] = []
    for cells in rows:
        if not cells:
            continue
        row_id = cells[0]  # ID column (seen in your screenshot)
        if not row_id or row_id in seen_ids:
            continue
        seen_ids.add(row_id)
        # Map likely columns from your screenshot
        item = {
            "id": row_id,
            "category": cells[1] if len(cells) > 1 else "",
            "color": cells[2] if len(cells) > 2 else "",
            "dimensions": cells[3] if len(cells) > 3 else "",
            "price": cells[4] if len(cells) > 4 else "",
            "product": cells[5] if len(cells) > 5 else "",
            "score": cells[6] if len(cells) > 6 else "",
            "cells": cells,  # keep raw cells too
        }
        new_items.append(item)
    return new_items


async def scroll_and_collect_all(page: Page, table_sel: str, target_count: int = 2849) -> List[Dict[str, Any]]:
    """
    Efficiently scroll a VIRTUALIZED table by scrolling its true scrollable ancestor.
    Continues "indefinitely" until the table stops loading more rows:
    - We detect exhaustion when the scroller is at bottom and no new rows appear for several rounds.
    - A large safety ceiling prevents literal infinity in case of a pathological page.
    """
    products: List[Dict[str, Any]] = []
    seen_ids: set = set()
    attempts = 0
    no_progress_rounds = 0
    last_count = 0
    safety_ceiling = 20000  # very high cap to avoid true infinites

    print(f"Starting collection (until exhausted). Target hint: {target_count}")

    # Ensure we start focused in the table region (some libs require focus)
    try:
        t = await page.query_selector(table_sel)
        if t:
            await t.click()
    except:
        pass

    # Reset scroller to top once before starting
    await page.evaluate("""
        (sel) => {
            const table = document.querySelector(sel);
            if (!table) return;
            const getScroller = (el) => {
                let node = el;
                while (node && node !== document.body) {
                    const s = getComputedStyle(node);
                    const oy = s.overflowY;
                    if ((oy === 'auto' || oy === 'scroll') && node.scrollHeight > node.clientHeight + 1) {
                        return node;
                    }
                    node = node.parentElement;
                }
                // fallback: search descendants (rare)
                for (const d of table.querySelectorAll('div')) {
                    const s2 = getComputedStyle(d);
                    const oy2 = s2.overflowY;
                    if ((oy2 === 'auto' || oy2 === 'scroll') && d.scrollHeight > d.clientHeight + 1) {
                        return d;
                    }
                }
                return null;
            };
            const scroller = getScroller(table);
            if (scroller) scroller.scrollTop = 0;
        }
    """, table_sel)

    while attempts < safety_ceiling:
        attempts += 1

        # Grab newly visible rows (single DOM call)
        new_rows = await extract_new_products(page, table_sel, seen_ids)
        if new_rows:
            products.extend(new_rows)
        
        # progress accounting
        if len(products) > last_count:
            last_count = len(products)
            no_progress_rounds = 0
        else:
            no_progress_rounds += 1

        if attempts % 25 == 0:
            pct = (len(products) / target_count * 100) if target_count else 0
            print(f"Progress: {len(products)} items collected ({pct:.1f}% est)")

        # Scroll inside the real scroller (NOT the page, NOT the <table>)
        scrolled = await page.evaluate("""
            (sel) => {
                const table = document.querySelector(sel);
                if (!table) return {ok:false, reason:'no-table'};
                const getScroller = (el) => {
                    let node = el;
                    while (node && node !== document.body) {
                        const s = getComputedStyle(node);
                        const oy = s.overflowY;
                        if ((oy === 'auto' || oy === 'scroll') && node.scrollHeight > node.clientHeight + 1) {
                            return node;
                        }
                        node = node.parentElement;
                    }
                    // fallback: look inside
                    for (const d of table.querySelectorAll('div')) {
                        const s2 = getComputedStyle(d);
                        const oy2 = s2.overflowY;
                        if ((oy2 === 'auto' || oy2 === 'scroll') && d.scrollHeight > d.clientHeight + 1) {
                            return d;
                        }
                    }
                    return null;
                };
                const scroller = getScroller(table);
                if (!scroller) return {ok:false, reason:'no-scroller'};

                const prev = scroller.scrollTop;
                const max = scroller.scrollHeight - scroller.clientHeight;
                // jump by one viewport
                const next = Math.min(prev + scroller.clientHeight, max);
                scroller.scrollTop = next;

                return {ok:true, prev, now: scroller.scrollTop, max};
            }
        """, table_sel)

        # Detect exhaustion: at bottom AND no new rows for a few rounds
        at_bottom = False
        if scrolled and scrolled.get("ok"):
            at_bottom = (scrolled.get("now", 0) >= scrolled.get("max", 0))

        # If we didn't scroll or we're at bottom, and no new rows for several rounds, stop
        if (not scrolled or not scrolled.get("ok") or scrolled.get("now") == scrolled.get("prev")):
            if at_bottom and no_progress_rounds >= 5:
                print("No more movement and no new rows; reached bottom. Stopping.")
                break

        # give virtualization time to render next batch
        await page.wait_for_timeout(200)

    print(f"Collection complete: {len(products)} products after {attempts} attempts (exhausted or safety cap)")
    return products


async def main() -> None:
    print("Starting optimized scraper")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS)
        context = None

        try:
            # Handle session storage
            if STORAGE_FILE.exists():
                context = await browser.new_context(storage_state=str(STORAGE_FILE))
                page = await context.new_page()
                await page.goto(TARGET_URL)

                if not await is_logged_in(page):
                    await context.close()
                    context = await browser.new_context()
                    page = await context.new_page()
                    await page.goto(TARGET_URL)
                    if await try_login(page):
                        await context.storage_state(path=str(STORAGE_FILE))
            else:
                context = await browser.new_context()
                page = await context.new_page()
                await page.goto(TARGET_URL)
                if not await is_logged_in(page):
                    if await try_login(page):
                        await context.storage_state(path=str(STORAGE_FILE))

            # Navigate through challenge steps
            print("Launching challenge...")
            await click_button_by_text(page, ["Launch Challenge", "Start Challenge"])
            await page.wait_for_timeout(800)

            print("Step 1: Selecting data source...")
            await complete_step(page, ["Local Database", "Local DB"])

            print("Step 2: Choosing category...")
            await complete_step(page)

            print("Step 3: Selecting view type...")
            await complete_step(page)

            print("Step 4: Loading products...")
            await complete_step(page)
            await click_button_by_text(page, ["View Products", "Finish", "Open"])

            # Wait for table to load
            print("Waiting for product table...")
            try:
                await page.wait_for_selector("table, [role=table], tbody tr", timeout=5000)
            except:
                print("Table load timeout - proceeding anyway")

            # We keep 'table' as selector and resolve the true scroller in JS during scroll
            table_sel = await find_table_container(page)
            print(f"Using table selector: {table_sel}")

            # Detect target count if available; ignore obviously wrong zeros
            target_count = 2849  # sensible default for your challenge
            page_text = await page.evaluate("() => document.body.innerText")
            match = re.search(r'showing\\s+\\d+\\s+of\\s+(\\d+)', page_text.lower())
            if match:
                detected = int(match.group(1))
                if detected > 0:
                    target_count = detected
                    print(f"Detected {target_count} total products")
                else:
                    print("Detected total=0 in banner; using default target_count")

            # Collect all products via virtual scroll
            products = await scroll_and_collect_all(page, table_sel, target_count)

            # Save results
            print(f"Saving {len(products)} products to {OUTPUT_FILE}")
            with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
                json.dump(products, f, indent=2, ensure_ascii=False)

            # Summary
            actual_count = len(products)
            completion_rate = (actual_count / target_count) * 100 if target_count else 0
            print(f"Collection complete: {actual_count}/{target_count} products ({completion_rate:.1f}%)")

        finally:
            if context:
                await context.close()
            await browser.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Interrupted by user")
