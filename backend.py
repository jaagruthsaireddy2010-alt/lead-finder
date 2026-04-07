from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

import re
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import quote_plus, unquote, urljoin

import csv
import requests
import httpx
from bs4 import BeautifulSoup

from fake_useragent import UserAgent

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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

EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")

SKIP_EMAILS = [
    "sentry@", "wix@", "example@", "test@", "noreply@", "no-reply@",
    "wordpress@", "changeme@", "yourname@", "user@", "admin@example"
]

SKIP_DOMAINS = [
    "facebook.com", "instagram.com", "twitter.com", "youtube.com",
    "linkedin.com", "wikipedia.org", "pinterest.com", "x.com"
]

LOW_VALUE_DOMAINS = [
    "practo.com", "lybrate.com", "apollo247.com", "clinicspots.com",
    "365doctor.in", "docgenie.in", "justdial.com", "sulekha.com",
    "asklaila.com", "threebestrated.in", "hexahealth.com",
    "doctor360.in", "shopkhoj.com", "localo.site",
    "maps.apple.com", "apple.com", "google.com/maps", "maps.google.", "bing.com/maps"
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
        self.lead_score = ""
        self.score_reasons = []
        self.source = ""
        self.has_website = False
        self.emails_found = []
        self.is_low_value = False

    def to_dict(self):
        em = self.email or (self.emails_found[0] if self.emails_found else "")
        return {
            "name": self.name,
            "address": self.address,
            "phone": self.phone,
            "email": em,
            "website": self.website if self.website else "-",
            "rating": self.rating,
            "reviews": self.review_count,
            "score": self.lead_score,
            "scoreReasons": self.score_reasons,
            "source": self.source,
            "hasWebsite": self.has_website,
        }


class SearchReq(BaseModel):
    location: str = "Yelachenahalli, Bangalore"
    businessType: str = "Dentist"
    radius: str = "5 km"
    depthMode: str = "Detailed"
    email: str = ""


def normalize_text(s: str) -> str:
    s = (s or "").lower().strip()
    s = s.replace("bengaluru", "bangalore")
    s = s.replace("jewellery", "jewelry")
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def get_business_keywords(business_type: str):
    """
    Returns: (include_keywords, exclude_keywords, brand_names)
    """
    bt = normalize_text(business_type)
    
    categories = {
        # Jewelry & Gold
        "jewelry": (
            ["jewel", "jewels", "jeweller", "jewellers", "jewellery", "jewelry", "jewelery",
             "gold", "silver", "diamond", "platinum", "ornament", "ornaments", "karat", "carat",
             "bangle", "bangles", "necklace", "chain", "pendant", "earring", "ring", "bracelet",
             "mangalsutra", "kada", "payal", "tikka", "nose pin", "studs", "goldsmith",
             "hallmark", "certified gold", "pure gold", "wedding jewelry", "bridal jewelry"],
            ["fashion store", "clothing store", "apparel", "textile", "garment", "boutique", "dress"],
            ["tanishq", "grt", "khazana", "grt khazana", "jos alukkas", "alukkas", "kalyan", "malabar",
             "bhima", "lalithaa", "lalitha", "joyalukkas", "bluestone", "caratlane",
             "orra", "senco", "pc jeweller", "tribhovandas bhimji zaveri", "tbz",
             "png", "popley", "waman hari pethe", "pmj", "rgold", "r gold", "zoya",
             "melorra", "amrapali", "apj", "reliance jewels", "candere"]
        ),
        
        "gold": (
            ["gold", "jewel", "jewellery", "jewelry", "jeweller", "goldsmith", "ornament",
             "silver", "diamond", "chain", "bangle", "necklace", "pendant", "ring"],
            ["fashion", "clothing", "boutique", "textile"],
            ["tanishq", "grt", "khazana", "kalyan", "malabar", "bhima", "lalithaa",
             "joyalukkas", "jos alukkas", "pmj", "rgold", "zoya", "amrapali"]
        ),
        
        # Health & Medical
        "clinic": (
            ["clinic", "clinics", "medical", "healthcare", "health center", "polyclinic",
             "multispeciality", "super speciality", "medical center", "nursing home", "doctor"],
            ["hospital chain main", "pharmacy only"],
            ["apollo clinic", "fortis clinic", "manipal clinic", "columbia asia"]
        ),
        
        "dental": (
            ["dental", "dentist", "dentistry", "orthodont", "teeth", "oral", "tooth",
             "denture", "braces", "smile", "root canal", "implant", "cosmetic dentistry"],
            [],
            ["clove dental", "sabka dentist", "apollo white dental"]
        ),
        
        "physio": (
            ["physio", "physiotherapy", "physiotherapist", "rehab", "rehabilitation",
             "physical therapy", "sports injury", "pain management", "spine care"],
            [],
            []
        ),
        
        "diagnostic": (
            ["diagnostic", "lab", "laboratory", "pathology", "radiology", "imaging",
             "scan", "ct scan", "mri", "xray", "blood test", "health checkup"],
            [],
            ["thyrocare", "dr lal pathlabs", "metropolis", "vijaya diagnostic"]
        ),
        
        "eye": (
            ["eye", "ophthal", "vision", "optical", "optician", "lasik", "cataract",
             "retina", "glaucoma", "eye care", "eye hospital"],
            ["sunglass", "fashion"],
            ["sankara nethralaya", "aravind eye", "narayana nethralaya"]
        ),
        
        "derma": (
            ["derma", "dermatology", "skin", "hair", "cosmetology", "aesthetic",
             "laser", "skin care", "hair care", "cosmetic"],
            [],
            ["kaya", "oliva"]
        ),
        
        # Fitness & Personal Care
        "gym": (
            ["gym", "gymnasium", "fitness", "workout", "training", "strength",
             "cardio", "weights", "bodybuilding", "crossfit", "fitness center"],
            [],
            ["cult fit", "gold gym", "golds gym", "anytime fitness", "talwalkars"]
        ),
        
        "yoga": (
            ["yoga", "yogasana", "pranayama", "meditation", "wellness", "yoga center",
             "yoga studio", "hatha", "ashtanga", "vinyasa", "power yoga"],
            [],
            ["isha yoga", "art of living"]
        ),
        
        "salon": (
            ["salon", "saloon", "beauty", "beauty parlor", "beauty parlour", "unisex salon",
             "hair salon", "hair studio", "styling", "spa salon"],
            [],
            ["lakme salon", "naturals", "tony and guy", "jawed habib", "green trends"]
        ),
        
        "barber": (
            ["barber", "barber shop", "gents salon", "men salon", "hair cut",
             "shaving", "grooming", "mens grooming"],
            ["ladies", "women"],
            ["truefitt and hill"]
        ),
        
        "spa": (
            ["spa", "massage", "thai spa", "ayurvedic spa", "wellness spa",
             "body massage", "aromatherapy"],
            [],
            ["tattva spa", "quan spa"]
        ),
        
        # Food
        "restaurant": (
            ["restaurant", "restaurants", "dining", "dine", "food", "cuisine", "eatery",
             "multi cuisine", "veg restaurant", "non veg", "family restaurant",
             "fine dining", "casual dining", "pure veg", "meals"],
            ["cloud kitchen only", "delivery only"],
            ["mtr", "vidyarthi bhavan", "mavalli tiffin room", "barbeque nation"]
        ),
        
        "cafe": (
            ["cafe", "coffee", "coffee shop", "bistro", "cafe restaurant", "coffee house",
             "espresso", "cappuccino", "latte"],
            ["cloud kitchen"],
            ["cafe coffee day", "ccd", "starbucks", "costa coffee", "third wave"]
        ),
        
        "bakery": (
            ["bakery", "bakeries", "cake", "cakes", "pastry", "pastries", "bread",
             "bake shop", "confectionery", "patisserie", "dessert"],
            [],
            ["monginis", "karachi bakery", "iyengar bakery"]
        ),
        
        # Education
        "tuition": (
            ["tuition", "tution", "coaching", "classes", "academy", "institute",
             "learning center", "education", "tutorial"],
            ["college", "university", "school main"],
            ["byju", "aakash", "allen"]
        ),
        
        "english": (
            ["english", "spoken english", "ielts", "toefl", "communication skills"],
            [],
            ["british council"]
        ),
        
        "computer": (
            ["computer", "it training", "software training", "programming", "coding",
             "web development"],
            ["college", "university"],
            ["niit", "aptech"]
        ),
        
        "music": (
            ["music", "music class", "music academy", "vocal", "singing", "guitar",
             "keyboard", "piano", "drums", "classical music"],
            ["store instrument"],
            []
        ),
        
        "dance": (
            ["dance", "dance class", "dance academy", "bharatanatyam", "kathak",
             "contemporary", "hip hop", "salsa"],
            ["fitness main", "zumba"],
            []
        ),
        
        # Home Services
        "plumber": (
            ["plumber", "plumbing", "pipe", "leak", "drainage", "sanitary"],
            [],
            ["urban company", "housejoy"]
        ),
        
        "electric": (
            ["electrician", "electrical", "wiring", "electric work"],
            ["electronics store"],
            ["urban company"]
        ),
        
        "cleaning": (
            ["cleaning", "clean", "housekeeping", "home cleaning", "deep cleaning"],
            [],
            ["urban company", "housejoy"]
        ),
        
        "pest": (
            ["pest", "pest control", "termite", "cockroach", "rodent", "fumigation"],
            [],
            ["rentokil", "hicare"]
        ),
        
        "ac": (
            ["ac", "air conditioner", "air conditioning", "hvac", "ac repair", "ac service"],
            ["showroom"],
            ["urban company"]
        ),
        
        # Automotive
        "garage": (
            ["garage", "car service", "car repair", "auto", "automobile", "mechanic",
             "workshop", "service center"],
            ["showroom", "dealer"],
            ["bosch", "carnation", "gomechanic"]
        ),
        
        "bike": (
            ["bike", "two wheeler", "motorcycle", "scooter", "bike service"],
            ["showroom", "dealer"],
            []
        ),
        
        "car wash": (
            ["car wash", "car cleaning", "car detailing", "auto spa"],
            [],
            []
        ),
        
        "driving": (
            ["driving", "driving school", "motor training", "learners"],
            [],
            []
        ),
        
        # Real Estate
        "real estate": (
            ["real estate", "realtor", "property", "broker", "agent", "consultant"],
            [],
            []
        ),
        
        # Retail
        "clothing": (
            ["clothing", "clothes", "garment", "apparel", "fashion", "boutique"],
            ["tailor only", "laundry"],
            ["westside", "lifestyle", "max", "pantaloons"]
        ),
        
        "mobile": (
            ["mobile", "phone", "smartphone", "cell", "mobile shop"],
            ["repair only"],
            ["poorvika", "sangeetha"]
        ),
        
        "electronic": (
            ["electronics", "electronic", "appliance", "gadget", "laptop"],
            ["repair"],
            ["croma", "reliance digital", "vijay sales"]
        ),
        
        "grocery": (
            ["grocery", "groceries", "supermarket", "kirana", "provision"],
            ["wholesale only"],
            ["more", "reliance fresh", "dmart"]
        ),
        
        "pharmacy": (
            ["pharmacy", "pharmacies", "medical store", "chemist", "drug store"],
            [],
            ["apollo pharmacy", "medplus"]
        ),
        
        # Events
        "photo": (
            ["photo", "photography", "photographer", "studio", "photo studio",
             "candid", "wedding photography"],
            ["printing only"],
            []
        ),
        
        "video": (
            ["video", "videography", "videographer", "cinematography"],
            [],
            []
        ),
        
        "wedding": (
            ["wedding", "wedding planner", "event planner", "marriage",
             "decorator", "wedding decorator"],
            ["hall only"],
            []
        ),
        
        "event": (
            ["event", "event management", "event organizer", "party planner"],
            [],
            []
        ),
        
        # Travel
        "travel": (
            ["travel", "travel agency", "tour", "tourism", "tour operator"],
            [],
            ["thomas cook", "cox and kings", "sotc"]
        ),
        
        "visa": (
            ["visa", "visa consultant", "immigration", "passport"],
            [],
            ["vfs global"]
        ),
    }
    
    # Match category
    for key, (includes, excludes, brands) in categories.items():
        if key in bt:
            return includes, excludes, brands
    
    # Default
    words = [w for w in bt.split() if len(w) > 2]
    return words, [], []


def expand_business_terms(business_type: str):
    bt = normalize_text(business_type)
    if not bt:
        return []

    terms = {bt}

    # Singular/plural
    if bt.endswith("s") and len(bt) > 3:
        terms.add(bt[:-1])
    else:
        terms.add(bt + "s")
    
    if bt.endswith("ies") and len(bt) > 4:
        terms.add(bt[:-3] + "y")

    replacements = [
        ("shop", "store"),
        ("store", "shop"),
        ("services", "service"),
        ("service", "services"),
        ("centre", "center"),
        ("center", "centre"),
        ("jewellery", "jewelry"),
        ("jewelry", "jewellery"),
    ]

    current = list(terms)
    for term in current:
        for a, b in replacements:
            if a in term:
                terms.add(term.replace(a, b))

    words = [w for w in bt.split() if w]
    if len(words) > 1:
        for w in words:
            if len(w) > 2:
                terms.add(w)

    # Add brands
    _, _, brands = get_business_keywords(business_type)
    for brand in brands[:10]:
        terms.add(brand)

    return [t.strip() for t in terms if t.strip()]


def ddg_maps(btype: str, loc: str):
    print(f"  [Maps] {btype} in {loc}")
    leads = []
    seen_names = set()

    terms = expand_business_terms(btype)
    _, _, brands = get_business_keywords(btype)

    search_terms = list(set(terms[:8] + brands[:5]))

    for kw in search_terms:
        q = f"{kw} in {loc}"
        try:
            with httpx.Client(
                headers={
                    "User-Agent": HEADERS["User-Agent"],
                    "Referer": "https://duckduckgo.com/",
                },
                timeout=10,
                follow_redirects=True,
            ) as c:
                tr = c.get(f"https://duckduckgo.com/?q={quote_plus(q)}&ia=maps")

                vm = re.search(r'vqd="([^"]+)"', tr.text) or re.search(r"vqd=([^&\"' ]+)", tr.text)
                if not vm:
                    continue

                mr = c.get(
                    "https://duckduckgo.com/local.js",
                    params={
                        "q": q,
                        "tg": "maps_places",
                        "rt": "D",
                        "mkexp": "b",
                        "wiki": "1",
                        "is_b": "1",
                        "vqd": vm.group(1),
                    },
                )

                if mr.status_code != 200:
                    continue

                try:
                    data = mr.json()
                except:
                    continue

                for r in data.get("results", []):
                    l = Lead()
                    l.name = (r.get("name") or "").strip()
                    l.address = (r.get("address") or "").strip()
                    l.phone = (r.get("phone") or "").strip()
                    l.source = "maps"

                    name_key = normalize_text(l.name)
                    if name_key in seen_names or len(name_key) < 3:
                        continue
                    if l.name:
                        seen_names.add(name_key)

                    w = r.get("website") or r.get("url") or ""
                    if w:
                        l.website = w
                        l.has_website = True

                    try:
                        l.rating = float(r.get("rating", 0) or 0)
                    except:
                        pass

                    try:
                        l.review_count = int(r.get("reviews", 0) or 0)
                    except:
                        pass

                    if l.name:
                        leads.append(l)

        except Exception as e:
            print(f"    [Maps] err: {e}")

        time.sleep(0.6)

    print(f"    [Maps] {len(leads)} found")
    return leads


def ddg_web(btype: str, loc: str):
    print(f"  [Web] {btype} in {loc}")
    leads = []
    seen = set()

    terms = expand_business_terms(btype)

    queries = []
    for term in terms[:6]:
        queries.extend([
            f"{term} in {loc}",
            f"{term} {loc}",
            f"best {term} in {loc}",
        ])

    session = requests.Session()

    for q in queries[:15]:
        try:
            rp = session.post(
                "https://html.duckduckgo.com/html/",
                data={"q": q},
                headers={
                    "User-Agent": HEADERS["User-Agent"],
                    "Content-Type": "application/x-www-form-urlencoded",
                },
                timeout=8
            )

            if rp.status_code != 200:
                continue

            for r in BeautifulSoup(rp.text, "lxml").select("div.result, div.web-result")[:15]:
                tt = r.select_one("a.result__a, h2.result__title a")
                if not tt:
                    continue

                title = tt.get_text(strip=True)
                link = tt.get("href", "")

                if any(d in link.lower() for d in SKIP_DOMAINS):
                    continue

                ck = re.sub(r"[^a-z0-9]", "", title.lower())
                if ck in seen or len(ck) < 3:
                    continue
                seen.add(ck)

                sn = r.select_one("a.result__snippet, div.result__snippet")
                snippet = sn.get_text(strip=True) if sn else ""

                l = Lead()
                l.name = title
                l.address = loc
                l.source = "web"

                pm = re.search(r"[\+]?[0-9][\d\s\-\(\)]{8,15}", snippet)
                if pm:
                    l.phone = pm.group().strip()

                if link and "duckduckgo" not in link:
                    ac = re.search(r"uddg=([^&]+)", link)
                    l.website = unquote(ac.group(1)) if ac else link
                    l.has_website = True

                leads.append(l)

        except Exception as e:
            print(f"    [Web] err: {e}")

        time.sleep(0.5)

    print(f"    [Web] {len(leads)} found")
    return leads


def bing_search(btype: str, loc: str):
    print(f"  [Bing] {btype} in {loc}")
    leads = []
    seen = set()
    terms = expand_business_terms(btype)

    queries = [f"{t} in {loc}" for t in terms[:5]]
    queries += [f"best {btype} in {loc}"]

    for query in queries[:8]:
        try:
            rp = requests.get(
                f"https://www.bing.com/search?q={quote_plus(query)}&count=25",
                headers={"User-Agent": ua.random},
                timeout=10,
            )
            if rp.status_code != 200:
                continue

            for it in BeautifulSoup(rp.text, "lxml").select("li.b_algo, div.b_algo")[:25]:
                tt = it.select_one("h2 a, h3 a")
                if not tt:
                    continue

                ti = tt.get_text(strip=True)
                lk = tt.get("href", "")

                if any(d in lk.lower() for d in SKIP_DOMAINS + ["bing.com"]):
                    continue
                if len(ti) < 3:
                    continue

                ck = re.sub(r"[^a-z0-9]", "", ti.lower())
                if ck in seen:
                    continue
                seen.add(ck)

                sn = it.select_one("p, div.b_caption p")
                snippet = sn.get_text(strip=True) if sn else ""

                l = Lead()
                l.name = ti
                l.address = loc
                l.source = "bing"

                pm = re.search(r"[\+]?[0-9][\d\s\-\(\)]{8,15}", snippet)
                if pm:
                    l.phone = pm.group().strip()

                if lk:
                    l.website = lk
                    l.has_website = True

                leads.append(l)

        except Exception as e:
            print(f"    [Bing] err: {e}")

        time.sleep(1)

    print(f"    [Bing] {len(leads)} found")
    return leads


def get_emails(url: str):
    ems = set()
    if not url or not url.startswith("http"):
        return []

    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    s = requests.Session()
    s.headers.update(HEADERS)
    s.verify = False

    pages = [url] + [urljoin(url, p) for p in ["/contact", "/contact-us", "/about", "/about-us"]]

    for pg in pages:
        try:
            rp = s.get(pg, timeout=8, allow_redirects=True)
            if rp.status_code != 200:
                continue

            found = EMAIL_RE.findall(rp.text)

            for a in BeautifulSoup(rp.text, "lxml").select('a[href^="mailto:"]'):
                found.append(a.get("href", "").replace("mailto:", "").split("?")[0])

            for em in found:
                em = em.lower().strip().rstrip(".")
                if re.match(r"^[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}$", em):
                    if any(s in em for s in SKIP_EMAILS):
                        continue
                    if em.endswith((".png", ".jpg", ".css", ".js")):
                        continue
                    if len(em) < 60:
                        ems.add(em)

            if ems:
                break

        except:
            continue

        time.sleep(0.5)

    return sorted(ems)[:3]


def enrich(leads):
    sites = [l for l in leads if l.website and l.website != "-"]
    print(f"  [Email] Checking {len(sites)} sites...")
    ct = 0

    for l in sites:
        try:
            if any(x in l.website.lower() for x in ["maps.apple.com", "google.com/maps", "maps.google.", "bing.com/maps"]):
                continue

            r = get_emails(l.website)
            if r:
                l.emails_found = r
                l.email = r[0]
                ct += 1
        except:
            pass

        time.sleep(0.3)

    print(f"    [Email] Found {ct}/{len(sites)}")
    return leads


def clean(leads, business_type=""):
    """
    Intelligent category-aware filtering with aggressive mall removal
    """
    cleaned = []
    
    bt_norm = normalize_text(business_type)
    include_kw, exclude_kw, brands = get_business_keywords(business_type)

    # Mall/complex patterns to aggressively filter
    MALL_PATTERNS = [
        " mall", "malls", " plaza", "plazas", "shopping complex", "shopping center",
        "trade center", "commercial complex", "city center", " forum", " nexus",
        "phoenix market", "mantri", "orion", "garuda", "royal meenakshi",
        "ub city", "lido", "rex walk", "safina plaza", "jayanagar shopping",
        "1 mg", "central mall", "mega mall"
    ]

    for l in leads:
        l.name = l.name.strip().title() if l.name else ""

        if not l.name or len(l.name) <= 2:
            continue

        if l.phone:
            d = re.sub(r"\D", "", l.phone)
            if len(d) >= 10:
                l.phone = f"+91-{d[-10:]}"

        if l.website and not l.website.startswith("http"):
            l.website = "https://" + l.website

        low_name = normalize_text(l.name)
        low_web = normalize_text(l.website or "")
        low_addr = normalize_text(l.address or "")
        all_text = f"{low_name} {low_web} {low_addr}"

        # CRITICAL: Aggressively remove malls/plazas/complexes
        is_searching_mall = any(x in bt_norm for x in ["mall", "plaza", "complex", "shopping center"])
        
        if not is_searching_mall:
            # Check if name contains mall/plaza/complex patterns
            is_mall = any(pattern in low_name for pattern in MALL_PATTERNS)
            
            if is_mall:
                # Double-check: is it a jewelry brand showroom inside a mall?
                if not any(brand in low_name or brand in low_web for brand in brands[:15]):
                    continue  # Skip this mall

        # Check category-specific exclusions
        if exclude_kw:
            should_exclude = False
            for exc in exclude_kw:
                if exc in all_text:
                    if not any(inc in all_text for inc in include_kw[:5]):
                        should_exclude = True
                        break
            
            if should_exclude:
                continue

        # For Maps results - trust them but still filter malls
        if l.source == "maps":
            pass  # Already passed mall filter above
        else:
            # For web/bing - require keyword match
            if not any(word in all_text for word in include_kw):
                if not any(brand in all_text for brand in brands):
                    continue

        # Mark low value
        l.is_low_value = False
        if any(dom in (l.website or "").lower() for dom in LOW_VALUE_DOMAINS):
            l.is_low_value = True

        cleaned.append(l)

    # Deduplication
    seen = {}
    for l in cleaned:
        key = (
            re.sub(r"[^a-z0-9]", "", normalize_text(l.name))[:50]
            + "|"
            + re.sub(r"[^a-z0-9]", "", normalize_text(l.address))[:50]
        )

        if key in seen:
            old = seen[key]
            if not old.phone and l.phone:
                old.phone = l.phone
            if not old.website and l.website:
                old.website = l.website
                old.has_website = True
            if not old.email and l.email:
                old.email = l.email
            if not old.rating and l.rating:
                old.rating = l.rating
            if not old.review_count and l.review_count:
                old.review_count = l.review_count
            if l.source not in old.source:
                old.source += f", {l.source}"
        else:
            seen[key] = l

    return list(seen.values())


def score(leads):
    for l in leads:
        p = 0
        r = []

        if getattr(l, "is_low_value", False):
            p -= 10
            r.append("Directory/listing site or map result, lower priority")

        if not l.has_website and not l.website:
            p += 40
            r.append("No website — needs online presence")
        elif l.has_website and not l.email:
            p += 15
            r.append("Website exists but no email found")

        if 0 < l.rating < 3.5:
            p += 20
            r.append(f"Low rating ({l.rating}★) — needs improvement")

        if l.review_count >= 50 and l.rating < 3.5:
            p += 25
            r.append("High reviews but low rating")

        if 0 < l.review_count < 5:
            p += 10
            r.append("Very few reviews — needs review generation")

        if not l.phone:
            p += 5
            r.append("No phone number found")

        if l.rating >= 4.5 and l.review_count >= 50 and l.has_website and l.email:
            p -= 30
            r.append("Strong online presence already")

        if p >= 30:
            l.lead_score = "High"
        elif p >= 10:
            l.lead_score = "Medium"
        else:
            l.lead_score = "Low"
            if not r:
                r = ["Good online presence"]

        l.score_reasons = r

    leads.sort(key=lambda x: {"High": 0, "Medium": 1, "Low": 2}.get(x.lead_score, 3))
    return leads


csv_path = None


@app.post("/api/find-leads")
def find_leads(req: SearchReq):
    global csv_path

    print(f"\n{'='*50}\nSearch: {req.businessType} in {req.location} | {req.depthMode}\n{'='*50}")

    all_l = []
    det = req.depthMode.lower() == "detailed"

    try:
        all_l.extend(ddg_maps(req.businessType, req.location))
    except Exception as e:
        print(f"ddg_maps err: {e}")

    try:
        all_l.extend(ddg_web(req.businessType, req.location))
    except Exception as e:
        print(f"ddg_web err: {e}")

    try:
        all_l.extend(bing_search(req.businessType, req.location))
    except Exception as e:
        print(f"bing_search err: {e}")

    print(f"  [Raw] total before clean: {len(all_l)}")

    all_l = clean(all_l, req.businessType)
    print(f"  [After clean] total: {len(all_l)}")

    if det and all_l:
        all_l = enrich(all_l)

    print(f"  [After enrich] total: {len(all_l)}")

    all_l = score(all_l)
    print(f"  [After score] total: {len(all_l)}")

    od = Path("output")
    od.mkdir(exist_ok=True)
    fp = od / f"leads_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    with open(fp, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["Score", "Name", "Address", "Phone", "Email", "Website", "Rating", "Reviews", "Source"]
        )
        w.writeheader()
        for l in all_l:
            w.writerow({
                "Score": l.lead_score,
                "Name": l.name,
                "Address": l.address,
                "Phone": l.phone,
                "Email": l.email or "",
                "Website": l.website or "NONE",
                "Rating": l.rating,
                "Reviews": l.review_count,
                "Source": l.source,
            })

    csv_path = fp

    sm = {
        "total": len(all_l),
        "high": sum(1 for l in all_l if l.lead_score == "High"),
        "medium": sum(1 for l in all_l if l.lead_score == "Medium"),
        "low": sum(1 for l in all_l if l.lead_score == "Low"),
        "noWebsite": sum(1 for l in all_l if not l.has_website),
        "withEmail": sum(1 for l in all_l if l.email or l.emails_found),
    }

    print(f"Done: {sm}\nCSV: {fp}")

    return {
        "leads": [l.to_dict() for l in all_l],
        "summary": sm,
        "csvFile": str(fp)
    }


@app.get("/api/download-csv")
def dl_csv():
    global csv_path
    if csv_path and csv_path.exists():
        return FileResponse(str(csv_path), filename=csv_path.name, media_type="text/csv")
    return JSONResponse({"error": "No CSV"}, status_code=404)


@app.get("/api/health")
def health():
    return {"status": "running"}


@app.get("/")
def serve_ui():
    return FileResponse("index.html")


if __name__ == "__main__":
    import uvicorn
    print("\n🔍 Lead Finder API → http://localhost:8000\n")
    uvicorn.run(app, host="0.0.0.0", port=8000)
