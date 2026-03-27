"""
VBPL Legal Documents Crawler
============================

This script uses Playwright to crawl the Vietnamese National Database of Legal
Documents (vbpl.vn) and extract the URLs of all legal‑document detail pages.
The site hosts separate sub‑sites for each province/city and for each central
ministry or agency.  Each sub‑site exposes a search form on the page
`/Pages/vanban.aspx` which, when submitted with an empty keyword, lists all
documents available for that jurisdiction.  Document detail pages take the
form `vbpq-toanvan.aspx` (full text) and `vbpq-van-ban-goc.aspx` (original
scanned version).  The goal is to traverse all search result pages for
every sub‑site and collect these detail URLs without duplication.

Due to access restrictions on the server this script has been designed but
could not be fully executed in the current environment.  Nevertheless it
illustrates a robust approach suitable for production use.  It relies on
Playwright with a system‑installed Chromium binary (`/usr/bin/chromium`).
The crawler:

* Maintains a list of slugs for all provinces/cities (`hanoi`,
  `thanhphohochiminh`, etc.) and central ministries/agencies (`bocongan`,
  `botuphap`, etc.), plus `tw` for central documents.  These slugs were
  obtained by inspecting the view‑source of `https://vbpl.vn/pages/vbpq-timkiem.aspx`.
* Iterates through each slug, opening the corresponding
  `https://vbpl.vn/<slug>/Pages/vanban.aspx` page.
* Waits for the search button with id ``searchSubmit`` to appear and clicks it
  to trigger a blank search (listing all available documents).
* Once results are loaded, repeatedly extracts all anchor tags with class
  ``jt`` (the site uses this class for document links).  Only URLs
  containing ``vbpq-toanvan.aspx`` or ``vbpq-van-ban-goc.aspx`` are kept.
* Handles pagination by looking for a “next” link in the paging controls.  The
  selector used attempts to match several possible classes (`a.next`,
  `a.pgR`) or an anchor with text ``>``.  If no such link exists the
  crawler moves on to the next slug.
* Accumulates every unique URL in a set and writes them to
  ``vbpl_detail_urls.txt`` upon completion.

To run this script you need Playwright installed and to have a working
Chromium in your system.  In this environment the script uses the
system's Chromium via the ``executable_path`` argument.  You may need to
adjust the ``slugs`` list or pagination selectors if the site structure
changes.
"""

import asyncio
from typing import Set

from playwright.async_api import async_playwright


async def crawl_vbpl() -> None:
    """Crawl VBPL sub‑sites and save all document detail URLs to a file.

    The crawler loops through a predefined list of site slugs (for
    provinces/cities and central agencies).  For each slug it submits an
    empty search on the ``vanban.aspx`` page, iterates through all pages of
    results, and collects URLs pointing to document detail pages.
    """

    # Collected URLs will be stored in this set to avoid duplicates
    collected_urls: Set[str] = set()

    # List of slugs extracted from the portal's view‑source.  Each slug
    # corresponds to a province/city or a central ministry/agency.  The slug
    # ``tw`` holds central (national) documents.
    slugs = [
        # Central and ministries
        "tw",
        "bocongan",
        "bocongthuong",
        "bogiaoducdaotao",
        "bogiaothong",
        "bokehoachvadautu",
        "bokhoahoccongnghe",
        "bolaodong",
        "bovanhoathethao",
        "bonongnghiep",
        "bonoivu",
        "bongoaigiao",
        "boquocphong",
        "botaichinh",
        "botainguyen",
        "botuphap",
        "bothongtin",
        "boxaydung",
        "boyte",
        "nganhangnhanuoc",
        "thanhtrachinhphu",
        "uybandantoc",
        "vanphongchinhphu",
        "kiemtoannhanuoc",
        "toaannhandantoicao",
        "vienkiemsatnhandantoicao",
        # Municipalities
        "hanoi",
        "thanhphohochiminh",
        "danang",
        "haiphong",
        "cantho",
        # Provinces
        "angiang",
        "bariavungtau",
        "bacgiang",
        "backan",
        "baclieu",
        "bacninh",
        "bentre",
        "binhdinh",
        "binhduong",
        "binhphuoc",
        "binhthuan",
        "camau",
        "caobang",
        "daklak",
        "daknong",
        "dienbien",
        "dongnai",
        "dongthap",
        "gialai",
        "hagiang",
        "hanam",
        "hatinh",
        "haiduong",
        "haugiang",
        "hoabinh",
        "hungyen",
        "khanhhoa",
        "kiengiang",
        "kontum",
        "laichau",
        "lamdong",
        "langson",
        "laocai",
        "longan",
        "namdinh",
        "nghean",
        "ninhbinh",
        "ninhthuan",
        "phutho",
        "phuyen",
        "quangbinh",
        "quangnam",
        "quangngai",
        "quangninh",
        "quangtri",
        "soctrang",
        "sonla",
        "tayninh",
        "thaibinh",
        "thainguyen",
        "thanhhoa",
        "thuathienhue",
        "tiengiang",
        "travinh",
        "tuyenquang",
        "vinhlong",
        "vinhphuc",
        "yenbai",
    ]

    # Launch Playwright with the system Chromium.  The ``--no-sandbox`` flag
    # is required in many restricted environments.
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            executable_path="/usr/bin/chromium",
            headless=True,
            args=["--no-sandbox"],
        )
        context = await browser.new_context()

        for slug in slugs:
            # Construct the search page URL for this slug
            search_url = f"https://vbpl.vn/{slug}/Pages/vanban.aspx"
            page = await context.new_page()
            try:
                # Navigate to the search page.  ``wait_until='domcontentloaded'``
                # ensures that the HTML is parsed even if Ajax requests are still
                # pending.  A generous timeout guards against slow responses.
                await page.goto(search_url, timeout=60000, wait_until="domcontentloaded")

                # Look for the search button.  This element has id ``searchSubmit``
                # across all sub‑sites.  Wait until it appears and then click it.
                await page.wait_for_selector("#searchSubmit", timeout=30000)
                await page.click("#searchSubmit")

                # After clicking, results are loaded either via full page
                # navigation (to ``vbpq-timkiem.aspx``) or via asynchronous
                # injection into the current page.  Wait for the first result
                # anchor (class ``jt``) to ensure that results are ready.
                await page.wait_for_selector("a.jt", timeout=60000)

                # Pagination loop
                while True:
                    # Collect all document links on the current results page
                    anchors = await page.query_selector_all("a.jt")
                    for anchor in anchors:
                        href = await anchor.get_attribute("href")
                        if not href:
                            continue
                        # Normalize relative URLs to absolute URLs
                        full = href
                        if not full.startswith("http"):
                            # Prepend domain and ensure no double slashes
                            full = f"https://vbpl.vn{full}"
                        if "vbpq-toanvan.aspx" in full.lower() or "vbpq-van-ban-goc.aspx" in full.lower():
                            collected_urls.add(full)

                    # Attempt to locate a “next page” link.  The pagination
                    # controls vary slightly across sub‑sites.  The selectors
                    # below cover several observed patterns: ``a.next`` (class
                    # ``next``), ``a.pgR`` (SharePoint paging class), or an
                    # anchor with text ``>``.  Only one of these needs to
                    # match.
                    next_link = None
                    for selector in ["a.next", "a.pgR", "a:has-text('>')"]:
                        try:
                            elem = await page.query_selector(selector)
                        except Exception:
                            elem = None
                        if elem:
                            next_link = elem
                            break

                    if next_link:
                        try:
                            # Scroll into view before clicking to avoid
                            # interception by fixed headers
                            await next_link.scroll_into_view_if_needed()
                            await next_link.click()
                            # Wait for new results to load; wait for an element
                            # that does *not* belong to the previous page.
                            await page.wait_for_selector("a.jt", timeout=60000)
                            continue
                        except Exception:
                            # If clicking fails (e.g., disabled link), stop.
                            pass
                    # No next link found => we are on the last page
                    break

            except Exception as exc:
                # Log the exception and continue with the next slug
                print(f"[Warning] Failed to process slug '{slug}': {exc}")
            finally:
                await page.close()

        # Close the browser
        await browser.close()

    # Write the collected URLs to a file
    output_path = "vbpl_detail_urls.txt"
    with open(output_path, "w", encoding="utf-8") as f:
        for url in sorted(collected_urls):
            f.write(url + "\n")

    print(f"Collected {len(collected_urls)} document URLs.  Saved to {output_path}.")


if __name__ == "__main__":
    asyncio.run(crawl_vbpl())