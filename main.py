from __future__ import annotations

import csv
import io
import os
import re
import smtplib
import ssl
import json
from email.message import EmailMessage
from typing import List, Dict, Any, Set
from datetime import datetime, timezone

import requests
import feedparser
import gspread
from google.oauth2.service_account import Credentials

# ================== CONFIG ==================
SPREADSHEET_NAME = os.getenv("SPREADSHEET_NAME", "BeatFindr Leads")
YOUR_YOUTUBE = "https://youtube.com/@mysterymusicbeats92"

# Public Reddit search (no auth). Be polite with a real UA.
REDDIT_SEARCH_URL = "https://www.reddit.com/search.json"
UA = {"User-Agent": "BeatFindrBot/1.1 (+contact: bot-notifications@example.com)"}

# Email credentials (set as GitHub Action secrets)
GMAIL_USER = os.environ["GMAIL_USER"]
GMAIL_APP_PASSWORD = os.environ["GMAIL_APP_PASSWORD"]
EMAIL_TO = os.environ["EMAIL_TO"]

# ---------- BROAD SEARCH QUERIES (seed terms we actively query on Reddit) ----------
# Includes your requested phrases + strong buyer/sync phrases.
SEARCH_QUERIES: List[str] = [
    "buying beats",
    "paying for beats",
    "buying instrumentals",
    "need beats for video",
    "looking for trap beats",
    "need hip hop instrumental",
    "commission a beat",
    "custom trap instrumental",
    "exclusive rights beat",
    "budget for beats",
    "sync licensing hip hop",
    "hip hop placement brief",
    "cinematic hip hop placement",
    "music supervisor hip hop",
    "looking for hip hop instrumental",
    "looking for trap instrumental",
    "need cinematic instrumental",
]

# ---------- SMART MATCHING LEXICONS ----------
# 1) Buyer / intent
INTENT_TERMS: Set[str] = {
    "buy", "buying", "purchase", "pay", "paying", "budget", "usd", "$",
    "need", "looking for", "seeking", "request", "requests", "commission",
    "hire", "hiring", "brief", "placement", "sync", "licensing", "supervisor",
    "exclusive rights", "non-exclusive", "nonexclusive", "custom",
}
# 2) Asset terms
ASSET_TERMS: Set[str] = {
    "beat", "beats", "instrumental", "instrumentals", "track", "tracks",
    "music", "cue", "cues", "score", "soundtrack",
}
# 3) Genre/style focus
GENRE_TERMS: Set[str] = {
    "trap", "hip hop", "hip-hop", "rap", "drill", "boom bap",
    "cinematic", "orchestral", "dark", "epic", "cinematic hip hop",
    "orchestral hip hop",
}
# 4) Use-cases / sync hints
USE_CASE_TERMS: Set[str] = {
    "for video", "for film", "for movie", "for short film", "for youtube",
    "for tiktok", "for ad", "for trailer", "for commercial", "for podcast",
    "for game", "for tv", "for tv show",
}
# 5) Obvious seller/self-promo noise
NEGATIVE_TERMS: Set[str] = {
    "check my beat", "my new beat", "free beats", "i sell beats", "selling beats",
    "beat store", "beatstars.com", "airbit.com", "type beat pack", "beat pack",
    "prod by", "producer tag", "stream my beat",
}

# ---------- Sync/Licensing RSS feeds (add more anytime) ----------
SYNC_RSS_FEEDS: List[str] = [
    "https://news.google.com/rss/search?q=sync+licensing+hip+hop",
    "https://news.google.com/rss/search?q=sync+licensing+trap",
    "https://news.google.com/rss/search?q=music+licensing+hip+hop",
    "https://news.google.com/rss/search?q=placement+opportunity+hip+hop",
    "https://news.google.com/rss/search?q=sync+brief+hip+hop",
    "https://news.google.com/rss/search?q=cinematic+hip+hop+placement",
    "https://news.google.com/rss/search?q=music+supervisor+looking+for+music+hip+hop",
    "https://news.google.com/rss/search?q=call+for+submissions+hip+hop+instrumental",
    "https://news.google.com/rss/search?q=music+wanted+hip+hop+instrumental",
]

# ================== GOOGLE SHEETS ==================
def get_sheet():
    creds_info_raw = os.environ["GOOGLE_CREDS_JSON"]
    creds_info = json.loads(creds_info_raw)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    client = gspread.authorize(creds)

    try:
        sh = client.open(SPREADSHEET_NAME)
    except gspread.SpreadsheetNotFound:
        sh = client.create(SPREADSHEET_NAME)

    ws = sh.sheet1
    header = ["Timestamp", "Source", "From/Author", "Title", "URL", "Tags"]
    values = ws.get_all_values()
    if not values or values[0] != header:
        if values:
            ws.clear()
        ws.append_row(header)
    return ws


def existing_urls(ws) -> Set[str]:
    urls: Set[str] = set()
    for row in ws.get_all_values()[1:]:
        if len(row) >= 5 and row[4]:
            urls.add(row[4])
    return urls

# ================== MATCHING HELPERS ==================
def normalize(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").lower()).strip()

def contains_any(text: str, terms: Set[str]) -> bool:
    return any(t in text for t in terms)

def looks_like_buyer(text: str) -> bool:
    # Buyer/intent + asset + (genre OR sync/use-case)
    intent = contains_any(text, INTENT_TERMS) or "$" in text
    asset  = contains_any(text, ASSET_TERMS)
    style  = contains_any(text, GENRE_TERMS)
    syncy  = contains_any(text, USE_CASE_TERMS) or contains_any(text, {"sync", "licensing", "placement", "brief"})
    return asset and intent and (style or syncy)

def is_noise(text: str) -> bool:
    return contains_any(text, NEGATIVE_TERMS)

# ================== REDDIT (public search) ==================
def search_reddit(limit_per_query: int = 25) -> List[Dict[str, Any]]:
    leads: List[Dict[str, Any]] = []
    for q in SEARCH_QUERIES:
        params = {"q": q, "sort": "new", "limit": str(limit_per_query), "t": "week", "include_over_18": "on"}
        try:
            r = requests.get(REDDIT_SEARCH_URL, headers=UA, params=params, timeout=20)
            if r.status_code != 200:
                print(f"[WARN] Reddit query failed for '{q}': {r.status_code}")
                continue
            items = r.json().get("data", {}).get("children", [])
            for it in items:
                d = it.get("data", {})
                title = d.get("title") or ""
                tl = normalize(title)
                if not title or is_noise(tl) or not looks_like_buyer(tl):
                    continue
                author = d.get("author") or "unknown"
                permalink = d.get("permalink") or ""
                url = f"https://www.reddit.com{permalink}" if permalink else (d.get("url") or "")
                if not url:
                    continue
                leads.append({
                    "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z"),
                    "source": "Reddit",
                    "who": f"u/{author}",
                    "title": title,
                    "url": url,
                    "tags": "reddit; " + q,
                })
        except Exception as e:
            print(f"[ERR] Reddit error for '{q}': {e}")
    return leads

# ================== SYNC / LICENSING via RSS ==================
def search_sync_rss() -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for feed_url in SYNC_RSS_FEEDS:
        try:
            feed = feedparser.parse(feed_url)
            source = getattr(feed.feed, "title", "Sync RSS")
            for entry in feed.entries:
                title = getattr(entry, "title", "") or ""
                link = getattr(entry, "link", "") or ""
                if not link:
                    continue
                tl = normalize(title)
                if is_noise(tl):
                    continue
                intentish = contains_any(tl, {"sync", "licensing", "placement", "brief", "supervisor", "call for submissions", "music wanted"})
                styleish  = contains_any(tl, GENRE_TERMS) or contains_any(tl, {"hiphop", "urban"})
                if not (intentish and styleish):
                    continue
                out.append({
                    "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z"),
                    "source": source,
                    "who": source,
                    "title": title,
                    "url": link,
                    "tags": "sync; rss",
                })
        except Exception as e:
            print(f"[WARN] RSS error for {feed_url}: {e}")
    return out

# ================== EMAIL SENDER ==================
def email_csv(new_rows: List[List[str]], summary_counts: Dict[str, int]):
    if not new_rows:
        print("No new rows to email.")
        return

    # Build CSV in memory
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Timestamp", "Source", "From/Author", "Title", "URL", "Tags"])
    writer.writerows(new_rows)
    csv_bytes = output.getvalue().encode("utf-8")

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    total = sum(summary_counts.values())
    subject = f"BeatFindr Leads ({total} new) – {now_str}"

    body_lines = [
        "New buyer/sync leads for trap / hip-hop / cinematic instrumentals.",
        "",
        "Totals this run: " + (", ".join([f"{k}: {v}" for k, v in summary_counts.items() if v]) if total else "0"),
        "",
        "CSV attached with all new items.",
        f"Your sound (quick ref): {YOUR_YOUTUBE}",
        "",
        "Tip: reply fast with 2–3 best links + a simple pricing grid (lease/exclusive) or a custom quote.",
        "— BeatFindr Bot",
    ]

    msg = EmailMessage()
    msg["From"] = GMAIL_USER
    msg["To"] = EMAIL_TO
    msg["Subject"] = subject
    msg.set_content("\n".join(body_lines))
    msg.add_attachment(csv_bytes, maintype="text", subtype="csv", filename="beatfindr_leads.csv")

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(GMAIL_USER, GMAIL_APP_PASSWORD)
        server.send_message(msg)
    print(f"📬 Emailed {total} new leads to {EMAIL_TO}")

# ================== MAIN RUN ==================
def run_once():
    ws = get_sheet()
    seen = existing_urls(ws)

    reddit_leads = search_reddit(limit_per_query=25)
    rss_leads = search_sync_rss()

    # Deduplicate against sheet
    new_items: List[Dict[str, Any]] = []
    for lead in reddit_leads + rss_leads:
        if lead["url"] in seen:
            continue
        new_items.append(lead)
        seen.add(lead["url"])

    # Write to sheet
    new_rows: List[List[str]] = [
        [l["timestamp"], l["source"], l["who"], l["title"], l["url"], l["tags"]]
        for l in new_items
    ]
    if new_rows:
        ws.append_rows(new_rows, value_input_option="RAW")
        print(f"✅ Added {len(new_rows)} new leads to sheet.")
    else:
        print("ℹ️ No new leads (or all dupes).")

    # Email summary
    counts = {
        "Reddit": sum(1 for r in new_rows if r[1] == "Reddit"),
        "Sync RSS": sum(1 for r in new_rows if r[1] != "Reddit"),
    }
    email_csv(new_rows, counts)

if __name__ == "__main__":
    print(f"Running BeatFindr at {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S %Z')}")
    run_once()
