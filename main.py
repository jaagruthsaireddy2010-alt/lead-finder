"""
LOCAL AI LEAD FINDER - FIXED VERSION
=====================================
NO ChromeDriver, NO Selenium, NO broken libraries
Just requests + httpx
"""

import re
import time
import json
import smtplib
import os
from datetime import datetime
from pathlib import Path
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from urllib.parse import urljoin, quote_plus

import requests
import httpx
from bs4 import BeautifulSoup
import pandas as pd
from fake_useragent import UserAgent
from colorama import init, Fore, Style

init(autoreset=True)

ua = UserAgent()

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml",
}

KEYWORD_EXPANSIONS = {
    "dentist": ["dentist", "dental clinic", "dental hospital"],
    "gym": ["gym", "fitness center", "health club"],
    "pet shop": ["pet shop", "pet store", "pet supplies"],
    "salon": ["salon", "beauty parlour", "hair salon"],
    "restaurant": ["restaurant", "eatery", "cafe"],
    "plumber": ["plumber", "plumbing services"],
    "electrician": ["electrician", "electrical services"],
    "doctor": ["doctor", "clinic", "medical center"],
    "lawyer": ["lawyer", "advocate", "law firm"],
    "tutor": ["tutor", "coaching center", "tuition classes"],
    "photographer": ["photographer", "photo studio"],
    "carpenter": ["carpenter", "furniture maker"],
    "ca": ["chartered accountant", "CA firm", "tax consultant"],
    "real estate": ["real estate agent", "property dealer"],
    "pharmacy": ["pharmacy", "medical store", "chemist"],
    "mechanic": ["car mechanic", "auto repair", "garage"],
    "hospital": ["hospital", "nursing home", "health center"],
}

EMAIL_REGEX = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}"
)

SKIP_EMAILS = [
    "sentry@", "wix@", "example@", "test@",
    "noreply@", "no-reply@", "wordpress@",
    "changeme@", "email@example", "yourname@",
    "support@wordpress", "user@", "admin@example",
]

SKIP_DOMAINS = [
    "facebook.com", "instagram.com", "twitter.com",
    "youtube.com", "linkedin.com", "wikipedia.org",
    "pinterest.com", "x.com",
]


class Lead:
    def __init__(self):
        self.name = ""
        self.address = ""
        self.phone = ""
        self.website = ""
        self.email = ""
        self.rating = 0.0
        self.review_count = 0
        self.top_reviews = []
        self.lead_score = ""
        self.score_reasons = []
        self.source = ""
        self.has_website = False
        self.emails_found = []


# ══════════════════════════════════════════════════
# SOURCE 1: DUCKDUCKGO MAPS (Direct HTTP — no library)
# ══════════════════════════════════════════════════

def search_ddg_maps(business_type, location):
    """Search DuckDuckGo Maps using direct HTTP calls."""
    print(
        f"\n{Fore.CYAN}🗺️  SOURCE 1: DuckDuckGo Maps"
        f"{Style.RESET_ALL}"
    )

    leads = []
    bt = business_type.lower().strip()
    keywords = KEYWORD_EXPANSIONS.get(bt, [bt])

    for kw in keywords:
        query = f"{kw} in {location}"
        print(f"   🔎 '{query}'", end="")

        try:
            # DuckDuckGo Maps API endpoint
            url = "https://duckduckgo.com/local.js"
            params = {
                "q": query,
                "tg": "maps_places",
                "rt": "D",
                "mkexp": "b",
                "wiki": "1",
                "is_b": "1",
            }

            client = httpx.Client(
                headers={
                    "User-Agent": HEADERS["User-Agent"],
                    "Referer": "https://duckduckgo.com/",
                },
                timeout=15,
                follow_redirects=True,
            )

            # First get the vqd token
            token_resp = client.get(
                f"https://duckduckgo.com/?q={quote_plus(query)}&ia=maps"
            )
            vqd_match = re.search(
                r'vqd="([^"]+)"', token_resp.text
            ) or re.search(
                r"vqd=([^&]+)", token_resp.text
            )

            if not vqd_match:
                # Try alternate method
                vqd_match = re.search(
                    r'vqd\\x3d([^\\]+)', token_resp.text
                )

            if vqd_match:
                vqd = vqd_match.group(1)
                params["vqd"] = vqd

                map_resp = client.get(url, params=params)

                if map_resp.status_code == 200:
                    try:
                        data = map_resp.json()
                        results = data.get("results", [])

                        count = 0
                        for r in results:
                            lead = Lead()
                            lead.name = r.get("name", "").strip()
                            lead.address = r.get(
                                "address", ""
                            ).strip()
                            lead.phone = r.get(
                                "phone", ""
                            ).strip()
                            lead.source = "ddg_maps"

                            # Website
                            web = r.get("website", "") or r.get(
                                "url", ""
                            )
                            if web:
                                lead.website = web
                                lead.has_website = True

                            # Rating
                            try:
                                lead.rating = float(
                                    r.get("rating", 0) or 0
                                )
                            except (ValueError, TypeError):
                                pass

                            # Reviews
                            try:
                                lead.review_count = int(
                                    r.get("reviews", 0) or 0
                                )
                            except (ValueError, TypeError):
                                pass

                            if lead.name:
                                leads.append(lead)
                                count += 1

                        print(
                            f" → {Fore.GREEN}{count} "
                            f"places{Style.RESET_ALL}"
                        )

                    except json.JSONDecodeError:
                        print(
                            f" → {Fore.YELLOW}No JSON"
                            f"{Style.RESET_ALL}"
                        )
                else:
                    print(
                        f" → {Fore.YELLOW}Status "
                        f"{map_resp.status_code}"
                        f"{Style.RESET_ALL}"
                    )
            else:
                print(
                    f" → {Fore.YELLOW}No token"
                    f"{Style.RESET_ALL}"
                )

            client.close()

        except Exception as e:
            print(
                f" → {Fore.RED}Error: {str(e)[:50]}"
                f"{Style.RESET_ALL}"
            )

        time.sleep(2)

    print(
        f"   {Fore.GREEN}✅ DDG Maps: "
        f"{len(leads)} leads{Style.RESET_ALL}"
    )
    return leads


# ══════════════════════════════════════════════════
# SOURCE 2: DUCKDUCKGO WEB SEARCH (Direct HTTP)
# ══════════════════════════════════════════════════

def search_ddg_web(business_type, location):
    """Search DuckDuckGo web using direct HTTP."""
    print(
        f"\n{Fore.CYAN}🔍 SOURCE 2: DuckDuckGo Web Search"
        f"{Style.RESET_ALL}"
    )

    leads = []
    bt = business_type.lower().strip()
    keywords = KEYWORD_EXPANSIONS.get(bt, [bt, f"{bt} services"])

    queries = []
    for kw in keywords:
        queries.append(f"{kw} in {location}")
        queries.append(f"best {kw} {location} contact number")

    seen_names = set()

    for query in queries:
        print(f"   🔎 '{query}'", end="")

        try:
            url = "https://html.duckduckgo.com/html/"
            resp = requests.post(
                url,
                data={"q": query},
                headers={
                    "User-Agent": ua.random,
                    "Content-Type": (
                        "application/x-www-form-urlencoded"
                    ),
                },
                timeout=15,
            )

            if resp.status_code != 200:
                print(
                    f" → {Fore.YELLOW}Status "
                    f"{resp.status_code}{Style.RESET_ALL}"
                )
                continue

            soup = BeautifulSoup(resp.text, "lxml")
            results = soup.select("div.result, div.web-result")

            count = 0
            for r in results[:20]:
                # Title
                title_tag = r.select_one(
                    "a.result__a, h2.result__title a"
                )
                if not title_tag:
                    continue

                title = title_tag.get_text(strip=True)
                link = title_tag.get("href", "")

                # Skip social media
                if any(d in link.lower() for d in SKIP_DOMAINS):
                    continue

                # Skip duplicates
                clean = re.sub(r"[^a-z0-9]", "", title.lower())
                if clean in seen_names or len(clean) < 3:
                    continue
                seen_names.add(clean)

                # Snippet
                snippet_tag = r.select_one(
                    "a.result__snippet, div.result__snippet"
                )
                snippet = (
                    snippet_tag.get_text(strip=True)
                    if snippet_tag else ""
                )

                lead = Lead()
                lead.name = title
                lead.source = "ddg_search"
                lead.address = location

                # Extract phone from snippet
                phone_match = re.search(
                    r"[\+]?[0-9][\d\s\-\(\)]{8,15}", snippet
                )
                if phone_match:
                    lead.phone = phone_match.group().strip()

                # Rating
                rat_match = re.search(
                    r"(\d\.\d)\s*(?:star|rating|/\s*5|★)",
                    snippet.lower(),
                )
                if rat_match:
                    try:
                        lead.rating = float(rat_match.group(1))
                    except ValueError:
                        pass

                # Website
                if link and "duckduckgo" not in link:
                    # DDG wraps URLs, extract actual URL
                    actual = re.search(
                        r"uddg=([^&]+)", link
                    )
                    if actual:
                        from urllib.parse import unquote
                        lead.website = unquote(actual.group(1))
                    else:
                        lead.website = link
                    lead.has_website = True

                leads.append(lead)
                count += 1

            print(
                f" → {Fore.GREEN}{count} results"
                f"{Style.RESET_ALL}"
            )

        except Exception as e:
            print(
                f" → {Fore.RED}Error: {str(e)[:50]}"
                f"{Style.RESET_ALL}"
            )

        time.sleep(2)

    print(
        f"   {Fore.GREEN}✅ DDG Search: "
        f"{len(leads)} leads{Style.RESET_ALL}"
    )
    return leads


# ══════════════════════════════════════════════════
# SOURCE 3: GOOGLE SEARCH SCRAPE (Direct HTTP)
# ══════════════════════════════════════════════════

def search_google(business_type, location):
    """Improved Google scrape with anti-detection."""
    print(
        f"\n{Fore.CYAN}🌐 SOURCE 3: Google Search"
        f"{Style.RESET_ALL}"
    )

    leads = []
    bt = business_type.lower().strip()

    queries = [
        f"{bt} in {location}",
        f"best {bt} near {location} phone number",
        f"{bt} {location} contact details reviews",
    ]

    for query in queries:
        print(f"   🔎 '{query}'", end="")

        try:
            session = requests.Session()

            # Realistic browser headers
            session.headers.update({
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                "Accept": (
                    "text/html,application/xhtml+xml,"
                    "application/xml;q=0.9,image/avif,"
                    "image/webp,image/apng,*/*;q=0.8"
                ),
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "DNT": "1",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1",
                "Cache-Control": "max-age=0",
                "Sec-Ch-Ua": (
                    '"Chromium";v="124", '
                    '"Google Chrome";v="124", '
                    '"Not-A.Brand";v="99"'
                ),
                "Sec-Ch-Ua-Mobile": "?0",
                "Sec-Ch-Ua-Platform": '"Windows"',
            })

            # Step 1: Visit google.com first (get cookies)
            session.get(
                "https://www.google.com/", timeout=10
            )
            time.sleep(2)

            # Step 2: Now search
            url = (
                f"https://www.google.com/search?"
                f"q={quote_plus(query)}&num=15&gl=in"
                f"&hl=en&pws=0&source=hp"
            )

            resp = session.get(url, timeout=12)

            if resp.status_code != 200:
                print(
                    f" → {Fore.YELLOW}Status "
                    f"{resp.status_code}{Style.RESET_ALL}"
                )
                time.sleep(5)
                continue

            html = resp.text

            # Check if Google blocked us
            if (
                "detected unusual traffic" in html.lower()
                or "captcha" in html.lower()
                or "sorry/index" in html.lower()
            ):
                print(
                    f" → {Fore.RED}BLOCKED (CAPTCHA)"
                    f"{Style.RESET_ALL}"
                )
                time.sleep(10)
                continue

            soup = BeautifulSoup(html, "lxml")
            count = 0

            # ── Method 1: Local Pack results ──
            local_divs = soup.select(
                "div.VkpGBb, div.rllt__details, "
                "div[class*='rllt'], div[data-attrid]"
            )

            for div in local_divs:
                name_tag = div.select_one(
                    "span.OSrXXb, div[role='heading'], "
                    "span.dbg0pd, span[class*='OSrXXb']"
                )
                if not name_tag:
                    continue

                name = name_tag.get_text(strip=True)
                if len(name) < 3:
                    continue

                lead = Lead()
                lead.name = name
                lead.source = "google_local"
                lead.address = location

                # Rating
                rat = div.select_one(
                    "span.yi40Hd, span.BTtC6e, "
                    "span[aria-label*='star'], "
                    "span[aria-label*='rating']"
                )
                if rat:
                    try:
                        txt = rat.get_text(strip=True)
                        txt = txt.replace(",", ".")
                        lead.rating = float(txt)
                    except ValueError:
                        pass

                # Reviews
                text = div.get_text()
                rev = re.search(r"\((\d[\d,]*)\)", text)
                if rev:
                    lead.review_count = int(
                        rev.group(1).replace(",", "")
                    )

                # Phone
                pm = re.search(
                    r"[\+]?[0-9][\d\s\-\(\)]{8,15}", text
                )
                if pm:
                    lead.phone = pm.group().strip()

                leads.append(lead)
                count += 1

            # ── Method 2: Organic results ──
            for result in soup.select("div.g, div.hlcw0c")[:15]:
                link_tag = result.select_one(
                    "a[href^='http']"
                )
                title_tag = result.select_one("h3")

                if not link_tag or not title_tag:
                    continue

                link = link_tag.get("href", "")
                title = title_tag.get_text(strip=True)

                if any(
                    d in link.lower() for d in SKIP_DOMAINS
                ):
                    continue
                if "google.com" in link.lower():
                    continue
                if len(title) < 3:
                    continue

                # Snippet
                snippet_tag = result.select_one(
                    "div.VwiC3b, span.aCOpRe, "
                    "div[data-sncf], div.IsZvec, "
                    "span[class*='st']"
                )
                snippet = (
                    snippet_tag.get_text(strip=True)
                    if snippet_tag else ""
                )

                lead = Lead()
                lead.name = title
                lead.website = link
                lead.has_website = True
                lead.address = location
                lead.source = "google_organic"

                # Phone from snippet
                pm = re.search(
                    r"[\+]?[0-9][\d\s\-\(\)]{8,15}",
                    snippet,
                )
                if pm:
                    lead.phone = pm.group().strip()

                # Rating from snippet
                rat_match = re.search(
                    r"(\d\.\d)\s*(?:star|rating|/\s*5|★)",
                    snippet.lower(),
                )
                if rat_match:
                    try:
                        lead.rating = float(
                            rat_match.group(1)
                        )
                    except ValueError:
                        pass

                leads.append(lead)
                count += 1

            # ── Method 3: Knowledge panel / sidebar ──
            for card in soup.select(
                "div.kp-blk, div[data-attrid*='title'], "
                "div.ifM9O"
            ):
                name_tag = card.select_one(
                    "h2, div[data-attrid*='title'] span, "
                    "div[role='heading']"
                )
                if not name_tag:
                    continue

                name = name_tag.get_text(strip=True)
                if len(name) < 3:
                    continue

                lead = Lead()
                lead.name = name
                lead.source = "google_panel"
                lead.address = location

                # Phone from card
                phone_tag = card.select_one(
                    "span[data-dtype='d3ph'], "
                    "a[href^='tel:'], "
                    "span[aria-label*='phone']"
                )
                if phone_tag:
                    if phone_tag.get("href", "").startswith(
                        "tel:"
                    ):
                        lead.phone = phone_tag["href"].replace(
                            "tel:", ""
                        )
                    else:
                        lead.phone = phone_tag.get_text(
                            strip=True
                        )

                # Website from card
                web_tag = card.select_one(
                    "a[data-dtype='d3web'], "
                    "a[class*='website']"
                )
                if web_tag:
                    lead.website = web_tag.get("href", "")
                    lead.has_website = True

                leads.append(lead)
                count += 1

            print(
                f" → {Fore.GREEN}{count} results"
                f"{Style.RESET_ALL}"
            )

        except Exception as e:
            print(
                f" → {Fore.RED}Error: {str(e)[:50]}"
                f"{Style.RESET_ALL}"
            )

        # Longer delay between Google queries
        time.sleep(5)

    print(
        f"   {Fore.GREEN}✅ Google: "
        f"{len(leads)} leads{Style.RESET_ALL}"
    )
    return leads


# ══════════════════════════════════════════════════
# SOURCE 4: JUSTDIAL
# ══════════════════════════════════════════════════

def search_justdial(business_type, location):
    """Scrape JustDial."""
    print(
        f"\n{Fore.CYAN}📒 SOURCE 4: JustDial"
        f"{Style.RESET_ALL}"
    )

    leads = []

    try:
        parts = location.split(",")
        city = parts[-1].strip().lower().replace(" ", "-")
        area = (
            parts[0].strip().lower().replace(" ", "-")
            if len(parts) > 1 else ""
        )
        btype = business_type.strip().lower().replace(" ", "-")

        urls = []
        if area:
            urls.append(
                f"https://www.justdial.com/{city}/{btype}/{area}"
            )
        urls.append(f"https://www.justdial.com/{city}/{btype}")

        session = requests.Session()
        session.headers.update({
            "User-Agent": ua.random,
            "Accept": "text/html",
            "Accept-Language": "en-US,en;q=0.9",
        })

        for url in urls:
            print(f"   🔎 {url}", end="")

            try:
                resp = session.get(url, timeout=15)

                if resp.status_code != 200:
                    print(
                        f" → {Fore.YELLOW}Status "
                        f"{resp.status_code}{Style.RESET_ALL}"
                    )
                    continue

                soup = BeautifulSoup(resp.text, "lxml")

                containers = (
                    soup.select("li.cntanr")
                    or soup.select("div.store-details")
                    or soup.select("div.resultbox_info")
                    or soup.select("div[class*='resultbox']")
                    or soup.select("div[class*='store']")
                )

                count = 0
                for c in containers[:25]:
                    lead = Lead()
                    lead.source = "justdial"

                    # Name
                    for sel in [
                        "span.lng_cont_name",
                        "a.lng_cont_name",
                        ".store-name span",
                        "a.jcn",
                        "p.resultbox_title_anchor",
                        "a[class*='title']",
                    ]:
                        tag = c.select_one(sel)
                        if tag:
                            lead.name = tag.get_text(strip=True)
                            break

                    if not lead.name:
                        continue

                    # Address
                    for sel in [
                        "span.cont_sw_addr",
                        "span.mln__address",
                        ".address-info",
                        "span.resultbox_address",
                    ]:
                        tag = c.select_one(sel)
                        if tag:
                            lead.address = tag.get_text(
                                strip=True
                            )
                            break

                    if not lead.address:
                        lead.address = location

                    # Rating
                    for sel in [
                        "span.green-box",
                        "span.lng_rat_cnt",
                        "span.resultbox_totalrate",
                    ]:
                        tag = c.select_one(sel)
                        if tag:
                            try:
                                lead.rating = float(
                                    tag.get_text(strip=True)
                                )
                            except ValueError:
                                pass
                            break

                    # Reviews
                    for sel in [
                        "span.rt_count",
                        "span.lng_vote_count",
                        "span.resultbox_countrate",
                    ]:
                        tag = c.select_one(sel)
                        if tag:
                            nums = re.findall(
                                r"\d+", tag.get_text()
                            )
                            if nums:
                                lead.review_count = int(nums[0])
                            break

                    # Phone
                    phone_tag = c.select_one("a[href^='tel:']")
                    if phone_tag:
                        lead.phone = phone_tag["href"].replace(
                            "tel:", ""
                        )

                    lead.has_website = False
                    leads.append(lead)
                    count += 1

                print(
                    f" → {Fore.GREEN}{count} listings"
                    f"{Style.RESET_ALL}"
                )
                if count > 0:
                    break

            except requests.RequestException as e:
                print(
                    f" → {Fore.RED}Failed{Style.RESET_ALL}"
                )

            time.sleep(2)

    except Exception as e:
        print(
            f"   {Fore.RED}❌ Error: {e}{Style.RESET_ALL}"
        )

    print(
        f"   {Fore.GREEN}✅ JustDial: "
        f"{len(leads)} leads{Style.RESET_ALL}"
    )
    return leads


# ══════════════════════════════════════════════════
# SOURCE 5: BING SEARCH (Another free source)
# ══════════════════════════════════════════════════

def search_bing(business_type, location):
    """Scrape Bing search results."""
    print(
        f"\n{Fore.CYAN}🔎 SOURCE 5: Bing Search"
        f"{Style.RESET_ALL}"
    )

    leads = []
    query = f"{business_type} in {location} contact"
    print(f"   🔎 '{query}'", end="")

    try:
        url = (
            f"https://www.bing.com/search?"
            f"q={quote_plus(query)}&count=20"
        )

        resp = requests.get(
            url,
            headers={
                "User-Agent": ua.random,
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=10,
        )

        if resp.status_code != 200:
            print(
                f" → {Fore.YELLOW}Status "
                f"{resp.status_code}{Style.RESET_ALL}"
            )
            return leads

        soup = BeautifulSoup(resp.text, "lxml")
        count = 0

        # Bing local results
        for item in soup.select(
            "div.b_scard, div.b_algo, li.b_algo"
        )[:20]:
            title_tag = item.select_one("h2 a, h3 a, a")
            if not title_tag:
                continue

            title = title_tag.get_text(strip=True)
            link = title_tag.get("href", "")

            if any(d in link.lower() for d in SKIP_DOMAINS):
                continue
            if "bing.com" in link.lower():
                continue
            if len(title) < 3:
                continue

            snippet_tag = item.select_one(
                "p, div.b_caption p, div.b_snippet"
            )
            snippet = (
                snippet_tag.get_text(strip=True)
                if snippet_tag else ""
            )

            lead = Lead()
            lead.name = title
            lead.address = location
            lead.source = "bing"

            # Phone
            pm = re.search(
                r"[\+]?[0-9][\d\s\-\(\)]{8,15}", snippet
            )
            if pm:
                lead.phone = pm.group().strip()

            if link:
                lead.website = link
                lead.has_website = True

            leads.append(lead)
            count += 1

        print(
            f" → {Fore.GREEN}{count} results"
            f"{Style.RESET_ALL}"
        )

    except Exception as e:
        print(
            f" → {Fore.RED}Error: {str(e)[:50]}"
            f"{Style.RESET_ALL}"
        )

    print(
        f"   {Fore.GREEN}✅ Bing: "
        f"{len(leads)} leads{Style.RESET_ALL}"
    )
    return leads


# ══════════════════════════════════════════════════
# EMAIL EXTRACTOR
# ══════════════════════════════════════════════════

def extract_emails(url):
    """Find emails on a website."""
    emails = set()
    if not url or not url.startswith("http"):
        return []

    import urllib3
    urllib3.disable_warnings(
        urllib3.exceptions.InsecureRequestWarning
    )

    session = requests.Session()
    session.headers.update(HEADERS)
    session.verify = False

    pages = [url]
    for path in [
        "/contact", "/contact-us", "/about", "/about-us"
    ]:
        pages.append(urljoin(url, path))

    for page in pages:
        try:
            resp = session.get(
                page, timeout=8, allow_redirects=True
            )
            if resp.status_code != 200:
                continue

            found = EMAIL_REGEX.findall(resp.text)

            soup = BeautifulSoup(resp.text, "lxml")
            for a in soup.select('a[href^="mailto:"]'):
                href = a.get("href", "")
                em = href.replace("mailto:", "").split("?")[0]
                found.append(em)

            for em in found:
                em = em.lower().strip().rstrip(".")
                if not re.match(
                    r"^[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}$",
                    em,
                ):
                    continue
                if any(s in em for s in SKIP_EMAILS):
                    continue
                if em.endswith(
                    (".png", ".jpg", ".gif", ".css", ".js")
                ):
                    continue
                if len(em) > 60:
                    continue
                emails.add(em)

            if emails:
                break

        except Exception:
            continue

        time.sleep(0.5)

    return sorted(emails)[:3]


def enrich_emails(leads):
    """Find emails for leads with websites."""
    sites = [l for l in leads if l.website]

    if not sites:
        print(
            f"\n{Fore.YELLOW}⚠️ No websites to check"
            f"{Style.RESET_ALL}"
        )
        return leads

    print(
        f"\n{Fore.CYAN}📧 Checking {len(sites)} "
        f"websites for emails...{Style.RESET_ALL}"
    )

    found = 0
    for i, lead in enumerate(sites):
        print(
            f"   [{i+1}/{len(sites)}] "
            f"{lead.website[:45]}",
            end=" ",
        )
        try:
            emails = extract_emails(lead.website)
            if emails:
                lead.emails_found = emails
                lead.email = emails[0]
                found += 1
                print(
                    f"→ {Fore.GREEN}✅ "
                    f"{emails[0]}{Style.RESET_ALL}"
                )
            else:
                print("→ ❌")
        except Exception:
            print("→ ⚠️")
        time.sleep(1)

    print(
        f"\n   {Fore.GREEN}📧 Emails found: "
        f"{found}/{len(sites)}{Style.RESET_ALL}"
    )
    return leads


# ══════════════════════════════════════════════════
# CLEANER
# ══════════════════════════════════════════════════

def normalize_phone(phone):
    if not phone:
        return ""
    digits = re.sub(r"\D", "", phone)
    if len(digits) >= 10:
        return f"+91-{digits[-10:]}"
    return phone


def clean_leads(leads):
    """Clean and deduplicate."""
    print(
        f"\n{Fore.CYAN}🧹 Cleaning {len(leads)} "
        f"raw leads...{Style.RESET_ALL}"
    )

    for lead in leads:
        lead.name = lead.name.strip().title() if lead.name else ""
        lead.phone = normalize_phone(lead.phone)
        if lead.website and not lead.website.startswith("http"):
            lead.website = "https://" + lead.website

    leads = [l for l in leads if l.name and len(l.name) > 2]

    # Deduplicate
    seen = {}
    for lead in leads:
        key = re.sub(r"[^a-z0-9]", "", lead.name.lower())[:25]
        if not key:
            continue

        if key in seen:
            old = seen[key]
            if not old.phone and lead.phone:
                old.phone = lead.phone
            if not old.website and lead.website:
                old.website = lead.website
                old.has_website = True
            if not old.email and lead.email:
                old.email = lead.email
            if not old.rating and lead.rating:
                old.rating = lead.rating
            if not old.review_count and lead.review_count:
                old.review_count = lead.review_count
            if lead.source not in old.source:
                old.source += f", {lead.source}"
        else:
            seen[key] = lead

    unique = list(seen.values())
    removed = len(leads) - len(unique)

    print(f"   Removed {removed} duplicates")
    print(
        f"   {Fore.GREEN}✅ {len(unique)} unique "
        f"leads{Style.RESET_ALL}"
    )
    return unique


# ══════════════════════════════════════════════════
# SCORER
# ══════════════════════════════════════════════════

def score_leads(leads):
    """Score each lead."""
    print(
        f"\n{Fore.CYAN}🤖 Scoring {len(leads)} "
        f"leads...{Style.RESET_ALL}"
    )

    high = medium = low = 0

    for lead in leads:
        points = 0
        reasons = []

        if not lead.has_website and not lead.website:
            points += 40
            reasons.append("NO WEBSITE — needs online presence")
        elif lead.has_website and not lead.email:
            points += 15
            reasons.append("Website but no email found")

        if 0 < lead.rating < 3.5:
            points += 20
            reasons.append(f"Low rating ({lead.rating}★)")

        if lead.review_count >= 50 and lead.rating < 3.5:
            points += 25
            reasons.append("Many reviews but low rating")

        if 0 < lead.review_count < 5:
            points += 10
            reasons.append("Very few reviews")

        if not lead.phone:
            points += 5
            reasons.append("No phone found")

        if (
            lead.rating >= 4.5
            and lead.review_count >= 50
            and lead.has_website
            and lead.email
        ):
            points -= 30
            reasons.append("Strong online presence already")

        if points >= 30:
            lead.lead_score = "HIGH"
            high += 1
        elif points >= 10:
            lead.lead_score = "MEDIUM"
            medium += 1
        else:
            lead.lead_score = "LOW"
            low += 1
            if not reasons:
                reasons.append("Good online presence")

        lead.score_reasons = reasons

    order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    leads.sort(key=lambda x: order.get(x.lead_score, 3))

    print(f"   🟢 HIGH:   {high}")
    print(f"   🟡 MEDIUM: {medium}")
    print(f"   🔵 LOW:    {low}")

    return leads


# ══════════════════════════════════════════════════
# CSV EXPORT
# ══════════════════════════════════════════════════

def export_csv(leads, business_type, location):
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_type = business_type.replace(" ", "_").lower()
    safe_loc = (
        location.split(",")[0].strip()
        .replace(" ", "_").lower()
    )
    filename = f"leads_{safe_type}_{safe_loc}_{timestamp}.csv"
    filepath = output_dir / filename

    rows = []
    for lead in leads:
        rows.append({
            "Lead Score": lead.lead_score,
            "Name": lead.name,
            "Address": lead.address,
            "Phone": lead.phone,
            "Email": lead.email or (
                "; ".join(lead.emails_found)
                if lead.emails_found else ""
            ),
            "Website": lead.website or "NONE",
            "Rating": lead.rating,
            "Reviews": lead.review_count,
            "Why This Lead": "; ".join(lead.score_reasons),
            "Source": lead.source,
        })

    df = pd.DataFrame(rows)
    df.to_csv(filepath, index=False, encoding="utf-8-sig")

    print(
        f"\n{Fore.GREEN}💾 CSV saved: "
        f"{filepath}{Style.RESET_ALL}"
    )
    print(f"   Rows: {len(df)}")
    return filepath


# ══════════════════════════════════════════════════
# EMAIL SENDER
# ══════════════════════════════════════════════════

def send_email(
    to_email, gmail_user, gmail_pass,
    business_type, location, summary, csv_path
):
    if not gmail_user or not gmail_pass:
        print(
            f"{Fore.YELLOW}⚠️ Skipping email"
            f"{Style.RESET_ALL}"
        )
        return

    print(
        f"\n{Fore.CYAN}📧 Sending to "
        f"{to_email}...{Style.RESET_ALL}"
    )

    try:
        msg = MIMEMultipart()
        msg["From"] = gmail_user
        msg["To"] = to_email
        msg["Subject"] = (
            f"AI Lead Results: {business_type.title()} "
            f"in {location}"
        )

        html = f"""
<html><body style="font-family:Arial;padding:20px;">
<h2>🎯 Lead Report: {business_type.title()} in {location}</h2>
<p>Total: <b>{summary['total']}</b> |
   High: <b style="color:green">{summary['high']}</b> |
   No Website: <b style="color:orange">{summary['no_website']}</b> |
   Emails: <b>{summary['with_email']}</b></p>
<p>📎 CSV attached.</p>
<p style="color:gray;font-size:11px;">
   Local AI Lead Finder (Free)</p>
</body></html>"""

        msg.attach(MIMEText(html, "html"))

        if csv_path.exists():
            with open(csv_path, "rb") as f:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(f.read())
                encoders.encode_base64(part)
                part.add_header(
                    "Content-Disposition",
                    f"attachment; filename={csv_path.name}",
                )
                msg.attach(part)

        with smtplib.SMTP("smtp.gmail.com", 587) as s:
            s.starttls()
            s.login(gmail_user, gmail_pass)
            s.send_message(msg)

        print(
            f"{Fore.GREEN}📧 ✅ Sent!{Style.RESET_ALL}"
        )
    except Exception as e:
        print(
            f"{Fore.RED}📧 ❌ Failed: {e}"
            f"{Style.RESET_ALL}"
        )


# ══════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════

def main():
    print(f"""
{Fore.CYAN}
╔═══════════════════════════════════════════════╗
║     🎯 LOCAL AI LEAD FINDER (FIXED)          ║
║                                               ║
║  ₹0 • No API Keys • No ChromeDriver          ║
║  No Selenium • No Broken Libraries            ║
╚═══════════════════════════════════════════════╝
{Style.RESET_ALL}""")

    print(f"{Fore.WHITE}━━━ Enter Details ━━━{Style.RESET_ALL}\n")

    location = input(
        "  📍 Location (e.g. Yelachenahalli, Bangalore): "
    ).strip()
    if not location:
        location = "Yelachenahalli, Bangalore"
        print(f"     → {location}")

    business_type = input(
        "  🏢 Business (e.g. Dentist, Gym): "
    ).strip()
    if not business_type:
        business_type = "Dentist"
        print(f"     → {business_type}")

    print(f"\n  ⚡ Mode:")
    print(f"     [1] Fast   (~1-2 min)")
    print(f"     [2] Detailed (~3-5 min)")
    mode = input("     Choose (1/2): ").strip()
    detailed = mode != "1"

    email_cfg = {}
    send = input(
        "\n  📧 Email results? (y/N): "
    ).strip().lower()
    if send == "y":
        email_cfg["to"] = input("     Your email: ").strip()
        email_cfg["gmail"] = input(
            "     Gmail (sender): "
        ).strip()
        email_cfg["pass"] = input(
            "     App Password (no spaces): "
        ).strip()

    print(f"\n{'━' * 50}")
    print(f"  Finding: {business_type} in {location}")
    print(f"{'━' * 50}")

    go = input(f"\n  🚀 Start? (Y/n): ").strip().lower()
    if go == "n":
        return

    start = time.time()
    all_leads = []

    # ── Collect from all sources ──────────────

    # Source 1: DDG Maps
    try:
        all_leads.extend(
            search_ddg_maps(business_type, location)
        )
    except Exception as e:
        print(f"{Fore.RED}DDG Maps error: {e}{Style.RESET_ALL}")

    # Source 2: DDG Web
    try:
        all_leads.extend(
            search_ddg_web(business_type, location)
        )
    except Exception as e:
        print(f"{Fore.RED}DDG Web error: {e}{Style.RESET_ALL}")

    # Source 3: Google
    try:
        all_leads.extend(
            search_google(business_type, location)
        )
    except Exception as e:
        print(f"{Fore.RED}Google error: {e}{Style.RESET_ALL}")

    # Source 4: JustDial (detailed)
    if detailed:
        try:
            all_leads.extend(
                search_justdial(business_type, location)
            )
        except Exception as e:
            print(
                f"{Fore.RED}JustDial error: {e}"
                f"{Style.RESET_ALL}"
            )

    # Source 5: Bing (detailed)
    if detailed:
        try:
            all_leads.extend(
                search_bing(business_type, location)
            )
        except Exception as e:
            print(
                f"{Fore.RED}Bing error: {e}"
                f"{Style.RESET_ALL}"
            )

    # ── Clean ─────────────────────────────────
    all_leads = clean_leads(all_leads)

    if not all_leads:
        print(
            f"\n{Fore.RED}❌ No leads found. "
            f"Try different terms.{Style.RESET_ALL}"
        )
        return

    # ── Emails ────────────────────────────────
    if detailed:
        all_leads = enrich_emails(all_leads)

    # ── Score ─────────────────────────────────
    all_leads = score_leads(all_leads)

    # ── Export ────────────────────────────────
    csv_path = export_csv(
        all_leads, business_type, location
    )

    # ── Summary ───────────────────────────────
    summary = {
        "total": len(all_leads),
        "high": sum(
            1 for l in all_leads if l.lead_score == "HIGH"
        ),
        "medium": sum(
            1 for l in all_leads if l.lead_score == "MEDIUM"
        ),
        "low": sum(
            1 for l in all_leads if l.lead_score == "LOW"
        ),
        "no_website": sum(
            1 for l in all_leads if not l.has_website
        ),
        "with_email": sum(
            1 for l in all_leads
            if l.email or l.emails_found
        ),
    }

    # ── Email (optional) ─────────────────────
    if email_cfg.get("to"):
        send_email(
            email_cfg["to"],
            email_cfg.get("gmail", ""),
            email_cfg.get("pass", ""),
            business_type, location, summary, csv_path,
        )

    # ── Final Report ─────────────────────────
    elapsed = time.time() - start

    print(f"\n")
    print(f"{Fore.GREEN}╔{'═' * 48}╗")
    print(f"║    🎯 COMPLETE!                              ║")
    print(f"╠{'═' * 48}╣")
    print(f"║  Total:          {summary['total']:>4}                        ║")
    print(f"║  🟢 High:        {summary['high']:>4}                        ║")
    print(f"║  🟡 Medium:      {summary['medium']:>4}                        ║")
    print(f"║  🔵 Low:         {summary['low']:>4}                        ║")
    print(f"║  🚫 No Website:  {summary['no_website']:>4}                        ║")
    print(f"║  📧 Emails:      {summary['with_email']:>4}                        ║")
    print(f"╠{'═' * 48}╣")
    print(f"║  📁 {str(csv_path):<43}║")
    print(f"║  ⏱  {elapsed:.0f}s | 💰 ₹0                           ║")
    print(f"╚{'═' * 48}╝{Style.RESET_ALL}")

    # Top leads
    high_leads = [
        l for l in all_leads if l.lead_score == "HIGH"
    ]
    if high_leads:
        print(
            f"\n{Fore.YELLOW}🔥 TOP LEADS:"
            f"{Style.RESET_ALL}"
        )
        print("─" * 50)
        for i, ld in enumerate(high_leads[:10], 1):
            web = (
                f"{Fore.RED}NO WEBSITE{Style.RESET_ALL}"
                if not ld.has_website
                else ld.website[:40]
            )
            print(
                f"  {i}. {Fore.WHITE}{ld.name}"
                f"{Style.RESET_ALL}"
            )
            print(f"     📍 {ld.address}")
            print(
                f"     📞 {ld.phone or 'N/A'} | "
                f"⭐ {ld.rating} ({ld.review_count})"
            )
            print(f"     🌐 {web}")
            if ld.email:
                print(f"     📧 {ld.email}")
            print(
                f"     💡 {'; '.join(ld.score_reasons[:2])}"
            )
            print()

    print(
        f"{Fore.CYAN}📁 Open CSV: {csv_path}"
        f"{Style.RESET_ALL}\n"
    )


if __name__ == "__main__":
    main()