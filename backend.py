from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel
import re, time, json, os
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, quote_plus, unquote
import requests, httpx
from bs4 import BeautifulSoup
import csv
from fake_useragent import UserAgent

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

ua = UserAgent()
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36", "Accept-Language": "en-US,en;q=0.9", "Accept": "text/html,application/xhtml+xml"}
KEYWORDS = {"dentist": ["dentist", "dental clinic", "dental hospital"], "dentists": ["dentist", "dental clinic"], "gym": ["gym", "fitness center", "health club"], "gyms": ["gym", "fitness center"], "pet shop": ["pet shop", "pet store"], "salon": ["salon", "beauty parlour", "hair salon"], "salons": ["salon", "beauty parlour"], "restaurant": ["restaurant", "eatery", "cafe"], "restaurants": ["restaurant", "cafe"], "plumber": ["plumber", "plumbing services"], "plumbers": ["plumber", "plumbing services"], "electrician": ["electrician", "electrical services"], "doctor": ["doctor", "clinic", "medical center"], "doctors": ["doctor", "clinic"], "lawyer": ["lawyer", "advocate", "law firm"], "tutor": ["tutor", "coaching center"], "photographer": ["photographer", "photo studio"], "pharmacy": ["pharmacy", "medical store", "chemist"], "mechanic": ["car mechanic", "auto repair"], "hospital": ["hospital", "nursing home"], "ca": ["chartered accountant", "CA firm"], "real estate": ["real estate agent", "property dealer"], "spa": ["spa", "massage", "beauty spa"]}
EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
SKIP_EM = ["sentry@","wix@","example@","test@","noreply@","no-reply@","wordpress@","changeme@","yourname@","user@","admin@example"]
SKIP_DOM = ["facebook.com","instagram.com","twitter.com","youtube.com","linkedin.com","wikipedia.org","pinterest.com","x.com"]

class Lead:
    def __init__(self):
        self.name=""; self.address=""; self.phone=""; self.website=""; self.email=""
        self.rating=0.0; self.review_count=0; self.lead_score=""; self.score_reasons=[]
        self.source=""; self.has_website=False; self.emails_found=[]
    def to_dict(self):
        em = self.email or (self.emails_found[0] if self.emails_found else "")
        return {"name":self.name,"address":self.address,"phone":self.phone,"email":em,"website":self.website if self.website else "-","rating":self.rating,"reviews":self.review_count,"score":self.lead_score,"scoreReasons":self.score_reasons,"source":self.source,"hasWebsite":self.has_website}

class SearchReq(BaseModel):
    location:str="Yelachenahalli, Bangalore"; businessType:str="Dentist"
    radius:str="5 km"; depthMode:str="Detailed"; email:str=""

def ddg_maps(btype, loc):
    print(f"  [Maps] {btype} in {loc}")
    leads=[]
    for kw in KEYWORDS.get(btype.lower().strip(), [btype.lower()]):
        q=f"{kw} in {loc}"
        try:
            c=httpx.Client(headers={"User-Agent":HEADERS["User-Agent"],"Referer":"https://duckduckgo.com/"},timeout=15,follow_redirects=True)
            tr=c.get(f"https://duckduckgo.com/?q={quote_plus(q)}&ia=maps")
            vm=re.search(r'vqd="([^"]+)"',tr.text) or re.search(r"vqd=([^&\"' ]+)",tr.text)
            if vm:
                mr=c.get("https://duckduckgo.com/local.js",params={"q":q,"tg":"maps_places","rt":"D","mkexp":"b","wiki":"1","is_b":"1","vqd":vm.group(1)})
                if mr.status_code==200:
                    try:
                        for r in mr.json().get("results",[]):
                            l=Lead(); l.name=r.get("name","").strip(); l.address=r.get("address","").strip()
                            l.phone=r.get("phone","").strip(); l.source="maps"
                            w=r.get("website","") or r.get("url","")
                            if w: l.website=w; l.has_website=True
                            try: l.rating=float(r.get("rating",0) or 0)
                            except: pass
                            try: l.review_count=int(r.get("reviews",0) or 0)
                            except: pass
                            if l.name: leads.append(l)
                    except: pass
            c.close()
        except Exception as e: print(f"  [Maps] err: {e}")
        time.sleep(2)
    print(f"  [Maps] {len(leads)} found")
    return leads

def ddg_web(btype, loc):
    print(f"  [Web] {btype} in {loc}")
    leads=[]; seen=set()
    for kw in KEYWORDS.get(btype.lower().strip(), [btype.lower()]):
        for q in [f"{kw} in {loc}", f"best {kw} {loc} contact number"]:
            try:
                rp=requests.post("https://html.duckduckgo.com/html/",data={"q":q},headers={"User-Agent":ua.random,"Content-Type":"application/x-www-form-urlencoded"},timeout=15)
                if rp.status_code!=200: continue
                for r in BeautifulSoup(rp.text,"lxml").select("div.result, div.web-result")[:20]:
                    tt=r.select_one("a.result__a, h2.result__title a")
                    if not tt: continue
                    title=tt.get_text(strip=True); link=tt.get("href","")
                    if any(d in link.lower() for d in SKIP_DOM): continue
                    ck=re.sub(r"[^a-z0-9]","",title.lower())
                    if ck in seen or len(ck)<3: continue
                    seen.add(ck)
                    sn=r.select_one("a.result__snippet, div.result__snippet")
                    snippet=sn.get_text(strip=True) if sn else ""
                    l=Lead(); l.name=title; l.source="web"; l.address=loc
                    pm=re.search(r"[\+]?[0-9][\d\s\-\(\)]{8,15}",snippet)
                    if pm: l.phone=pm.group().strip()
                    if link and "duckduckgo" not in link:
                        ac=re.search(r"uddg=([^&]+)",link)
                        l.website=unquote(ac.group(1)) if ac else link
                        l.has_website=True
                    leads.append(l)
            except Exception as e: print(f"  [Web] err: {e}")
            time.sleep(2)
    print(f"  [Web] {len(leads)} found")
    return leads

def google_search(btype, loc):
    print(f"  [Google] {btype} in {loc}")
    leads=[]; s=requests.Session(); s.headers.update({"User-Agent":ua.random,"Accept-Language":"en-US,en;q=0.9","Accept":"text/html"})
    for q in [f"{btype} in {loc}", f"best {btype} near {loc} phone"]:
        try:
            rp=s.get(f"https://www.google.com/search?q={quote_plus(q)}&num=15&gl=in",timeout=10)
            if rp.status_code!=200: continue
            soup=BeautifulSoup(rp.text,"lxml")
            for div in soup.select("div.VkpGBb, div.rllt__details"):
                nt=div.select_one("span.OSrXXb, div[role='heading'], span.dbg0pd")
                if not nt: continue
                nm=nt.get_text(strip=True)
                if len(nm)<3: continue
                l=Lead(); l.name=nm; l.source="google"; l.address=loc
                rt=div.select_one("span.yi40Hd, span.BTtC6e")
                if rt:
                    try: l.rating=float(rt.get_text(strip=True))
                    except: pass
                rv=re.search(r"\((\d+)\)",div.get_text())
                if rv: l.review_count=int(rv.group(1))
                leads.append(l)
            for r in soup.select("div.g")[:15]:
                lt=r.select_one("a[href^='http']"); ht=r.select_one("h3")
                if not lt or not ht: continue
                lk=lt.get("href",""); ti=ht.get_text(strip=True)
                if any(d in lk.lower() for d in SKIP_DOM+["google.com"]): continue
                l=Lead(); l.name=ti; l.website=lk; l.has_website=True; l.address=loc; l.source="google"
                leads.append(l)
        except Exception as e: print(f"  [Google] err: {e}")
        time.sleep(3)
    print(f"  [Google] {len(leads)} found")
    return leads

def bing_search(btype, loc):
    print(f"  [Bing] {btype} in {loc}")
    leads=[]
    try:
        rp=requests.get(f"https://www.bing.com/search?q={quote_plus(f'{btype} in {loc} contact')}&count=20",headers={"User-Agent":ua.random},timeout=10)
        if rp.status_code!=200: return leads
        for it in BeautifulSoup(rp.text,"lxml").select("li.b_algo, div.b_algo")[:20]:
            tt=it.select_one("h2 a, h3 a")
            if not tt: continue
            ti=tt.get_text(strip=True); lk=tt.get("href","")
            if any(d in lk.lower() for d in SKIP_DOM+["bing.com"]) or len(ti)<3: continue
            sn=it.select_one("p, div.b_caption p")
            snippet=sn.get_text(strip=True) if sn else ""
            l=Lead(); l.name=ti; l.address=loc; l.source="bing"
            pm=re.search(r"[\+]?[0-9][\d\s\-\(\)]{8,15}",snippet)
            if pm: l.phone=pm.group().strip()
            if lk: l.website=lk; l.has_website=True
            leads.append(l)
    except Exception as e: print(f"  [Bing] err: {e}")
    print(f"  [Bing] {len(leads)} found")
    return leads

def get_emails(url):
    ems=set()
    if not url or not url.startswith("http"): return []
    import urllib3; urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    s=requests.Session(); s.headers.update(HEADERS); s.verify=False
    for pg in [url]+[urljoin(url,p) for p in ["/contact","/contact-us","/about","/about-us"]]:
        try:
            rp=s.get(pg,timeout=8,allow_redirects=True)
            if rp.status_code!=200: continue
            found=EMAIL_RE.findall(rp.text)
            for a in BeautifulSoup(rp.text,"lxml").select('a[href^="mailto:"]'):
                found.append(a.get("href","").replace("mailto:","").split("?")[0])
            for em in found:
                em=em.lower().strip().rstrip(".")
                if re.match(r"^[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}$",em) and not any(s in em for s in SKIP_EM) and not em.endswith((".png",".jpg",".css",".js")) and len(em)<60:
                    ems.add(em)
            if ems: break
        except: continue
        time.sleep(0.5)
    return sorted(ems)[:3]

def enrich(leads):
    sites=[l for l in leads if l.website and l.website!="-"]
    print(f"  [Email] Checking {len(sites)} sites...")
    ct=0
    for l in sites:
        try:
            r=get_emails(l.website)
            if r: l.emails_found=r; l.email=r[0]; ct+=1
        except: pass
        time.sleep(0.5)
    print(f"  [Email] Found {ct}/{len(sites)}")
    return leads

def clean(leads):
    for l in leads:
        l.name=l.name.strip().title() if l.name else ""
        if l.phone:
            d=re.sub(r"\D","",l.phone)
            l.phone=f"+91-{d[-10:]}" if len(d)>=10 else l.phone
        if l.website and not l.website.startswith("http"): l.website="https://"+l.website
    leads=[l for l in leads if l.name and len(l.name)>2]
    seen={}
    for l in leads:
        k=re.sub(r"[^a-z0-9]","",l.name.lower())[:25]
        if not k: continue
        if k in seen:
            o=seen[k]
            if not o.phone and l.phone: o.phone=l.phone
            if not o.website and l.website: o.website=l.website; o.has_website=True
            if not o.email and l.email: o.email=l.email
            if not o.rating and l.rating: o.rating=l.rating
            if not o.review_count and l.review_count: o.review_count=l.review_count
            if l.source not in o.source: o.source+=f", {l.source}"
        else: seen[k]=l
    return list(seen.values())

def score(leads):
    for l in leads:
        p=0; r=[]
        if not l.has_website and not l.website: p+=40; r.append("No website — needs online presence")
        elif l.has_website and not l.email: p+=15; r.append("Website exists but no email found")
        if 0<l.rating<3.5: p+=20; r.append(f"Low rating ({l.rating}★) — needs improvement")
        if l.review_count>=50 and l.rating<3.5: p+=25; r.append("High reviews but low rating")
        if 0<l.review_count<5: p+=10; r.append("Very few reviews — needs review generation")
        if not l.phone: p+=5; r.append("No phone number found")
        if l.rating>=4.5 and l.review_count>=50 and l.has_website and l.email: p-=30; r.append("Strong online presence already")
        if p>=30: l.lead_score="High"
        elif p>=10: l.lead_score="Medium"
        else: l.lead_score="Low"; r=r or ["Good online presence"]
        l.score_reasons=r
    leads.sort(key=lambda x:{"High":0,"Medium":1,"Low":2}.get(x.lead_score,3))
    return leads

csv_path=None

@app.post("/api/find-leads")
def find_leads(req: SearchReq):
    global csv_path
    print(f"\n{'='*50}\nSearch: {req.businessType} in {req.location} | {req.depthMode}\n{'='*50}")
    all_l=[]
    det=req.depthMode.lower()=="detailed"
    try: all_l.extend(ddg_maps(req.businessType,req.location))
    except Exception as e: print(f"Maps err: {e}")
    if det:
        try: all_l.extend(bing_search(req.businessType,req.location))
        except Exception as e: print(f"Bing err: {e}")
    all_l=clean(all_l)
    if det and all_l: all_l=enrich(all_l)
    all_l = score(all_l)
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
    }
    print(f"Done: {sm}\nCSV: {fp}")
    return {"leads": [l.to_dict() for l in all_l], "summary": sm, "csvFile": str(fp)}

@app.get("/api/download-csv")
def dl_csv():
    global csv_path
    if csv_path and csv_path.exists(): return FileResponse(str(csv_path),filename=csv_path.name,media_type="text/csv")
    return JSONResponse({"error":"No CSV"},status_code=404)

@app.get("/api/health")
def health(): return {"status":"running"}
@app.get("/")
def serve_ui():
    return FileResponse("index.html")

if __name__=="__main__":
    import uvicorn
    print("\n  Lead Finder API → http://localhost:8000\n")
    uvicorn.run(app,host="0.0.0.0",port=8000)