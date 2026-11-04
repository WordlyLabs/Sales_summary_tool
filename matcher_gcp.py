"""
PROJECT: Wordly Sales Intelligence Pipeline
SCRIPT:  matcher_gcp.py
VERSION: 3.1
CHANGES: - src/ folder for processed_log.json, salespeople.csv, management_review.csv
         - prompts/ folder for all prompt files
         - Company and deal lookup from HubSpot meeting associations
         - Company name in filename: YYYY-MM-DD_HHMM_CompanyName_TYPE.txt
         - Management review CSV written to src/ folder
         - Suppress Slack notifications during backfill runs
         - Fixed processed_log deduplication (writes to src/, not summaries/)
         - Deal stage passed as context to Gemini prompts
AUTHOR:  Built with Claude
DATE:    2026-07-23
"""

import os
import io
import csv
import json
import sys
import time
import re
import requests
from datetime import datetime, timezone, timedelta
from collections import Counter

from google.cloud import secretmanager
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
GCP_PROJECT        = "support-467322"

GDRIVE_PROMPTS_FOLDER_ID      = os.environ.get("GDRIVE_PROMPTS_FOLDER_ID",      "1YUPG28Giyn7v7HogOeE7L5RYtJtBpjY1")
GDRIVE_SRC_FOLDER_ID          = os.environ.get("GDRIVE_SRC_FOLDER_ID",          "1kaObYZQ9rW1KL76xQ-GQGF1w_pvb_kNh")
GDRIVE_HS_FOLDER_ID           = os.environ.get("GDRIVE_HS_FOLDER_ID",           "1bMycbVJajJgchxGPsWf6n2lpk3Ku32nJ")
GDRIVE_AUDITS_FOLDER_ID       = os.environ.get("GDRIVE_AUDITS_FOLDER_ID",       "1GzZuflXocfacTFxjNPKXMWHJ58iUm1Fn")
GDRIVE_COMPETITIVE_FOLDER_ID  = os.environ.get("GDRIVE_COMPETITIVE_FOLDER_ID",  "1a5LiCw49p0PRzbsJjZbr67jvc25Q7K-f")
GDRIVE_ROADMAP_FOLDER_ID      = os.environ.get("GDRIVE_ROADMAP_FOLDER_ID",      "18FR5sc4-nn0bkMU641Y56U7Q81-5mu_d")
GDRIVE_TRANSCRIPTS_FOLDER_ID  = os.environ.get("GDRIVE_TRANSCRIPTS_FOLDER_ID",  "1IVN-QEk0nyO3AHQGXvV5v_3XE_gUaiZR")
GDRIVE_INTEL_FOLDER_ID        = os.environ.get("GDRIVE_INTEL_FOLDER_ID",        "1MLjzrE3QYpVfYKve42sJWCq0DmHDwMo4")

SECRET_HS_KEY    = "hs-service-key"
SECRET_GEMINI    = "gemini-api-key"
SECRET_SLACK     = "slack-webhook"
SECRET_SLACK_INTEL = "slack-webhook-intel"

WORDLY_BASE_URL  = "https://api.wordly.ai"
HS_BASE_URL      = "https://api.hubapi.com"
GEMINI_URL       = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent"

TARGET_REP       = None
LOOKBACK_HOURS   = 12
BACKFILL_DAYS    = 1
IS_BACKFILL      = os.environ.get("IS_BACKFILL", "false").lower() == "true"

MATCH_WINDOW_MINS  = 20
HIGH_THRESHOLD     = 6
MIN_DURATION_MINS  = 5
HS_PORTAL_ID       = "5315820"

WCS_OBJECT_TYPE_ID = "2-68535024"
WCS_ASSOC_CONTACT  = 349
WCS_ASSOC_COMPANY  = 345

WCS_OBJECT_TYPE_ID = "2-68535024"   # Wordly Call Summary custom object
WCS_ASSOC_CONTACT  = 349            # association type: WCS → contact
WCS_ASSOC_COMPANY  = 345            # association type: WCS → company

PROCESSED_FILENAME    = "processed_log.json"
REVIEW_CSV_FILENAME   = "management_review.csv"
SALESPEOPLE_FILENAME  = "salespeople.csv"

# ---------------------------------------------------------------------------
# SECRET MANAGER
# ---------------------------------------------------------------------------

def get_secret(secret_id):
    client = secretmanager.SecretManagerServiceClient()
    name   = f"projects/{GCP_PROJECT}/secrets/{secret_id}/versions/latest"
    try:
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("utf-8").strip()
    except Exception as e:
        print(f"  ❌  Secret Manager error for {secret_id}: {e}")
        return None


def get_wordly_key(email):
    local     = email.split("@")[0].replace(".", "-").lower()
    secret_id = f"wordly-key-{local}"
    return get_secret(secret_id)


# ---------------------------------------------------------------------------
# GOOGLE DRIVE
# ---------------------------------------------------------------------------

def get_drive_service():
    from google.auth import default
    creds, _ = default(scopes=["https://www.googleapis.com/auth/drive"])
    return build("drive", "v3", credentials=creds)


def drive_find_file(service, filename, parent_id, retries=3):
    import time as _time
    query = f"name='{filename}' and '{parent_id}' in parents and trashed=false"
    for attempt in range(retries):
        try:
            results = service.files().list(
                q=query, fields="files(id, name)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True
            ).execute()
            files = results.get("files", [])
            return files[0]["id"] if files else None
        except Exception as e:
            if "429" in str(e) and attempt < retries - 1:
                _time.sleep(2 ** attempt)
                continue
            return None


def drive_read_text(service, file_id):
    request    = service.files().get_media(fileId=file_id)
    buf        = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return buf.getvalue().decode("utf-8")


def drive_write_text(service, filename, content, parent_id, existing_file_id=None):
    buf   = io.BytesIO(content.encode("utf-8"))
    media = MediaIoBaseUpload(buf, mimetype="text/plain", resumable=False)
    if existing_file_id:
        updated = service.files().update(
            fileId=existing_file_id,
            media_body=media,
            supportsAllDrives=True
        ).execute()
        return updated["id"]
    else:
        metadata = {"name": filename, "parents": [parent_id]}
        created  = service.files().create(
            body=metadata, media_body=media, fields="id",
            supportsAllDrives=True
        ).execute()
        return created["id"]


def drive_get_or_create_folder(service, folder_name, parent_id):
    query   = (f"name='{folder_name}' and '{parent_id}' in parents "
               f"and mimeType='application/vnd.google-apps.folder' and trashed=false")
    results = service.files().list(
        q=query, fields="files(id)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True
    ).execute()
    files = results.get("files", [])
    if files:
        return files[0]["id"]
    metadata = {
        "name":     folder_name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents":  [parent_id]
    }
    folder = service.files().create(
        body=metadata, fields="id",
        supportsAllDrives=True
    ).execute()
    return folder["id"]


# ---------------------------------------------------------------------------
# LOAD / SAVE CONFIG
# ---------------------------------------------------------------------------

def load_salespeople(service):
    file_id = drive_find_file(service, SALESPEOPLE_FILENAME, GDRIVE_SRC_FOLDER_ID)
    if not file_id:
        print(f"  ❌  {SALESPEOPLE_FILENAME} not found in src/")
        return []
    content = drive_read_text(service, file_id)
    people  = []
    reader  = csv.DictReader(io.StringIO(content))
    for row in reader:
        active       = row.get("active", "false").strip().lower() in ("true","1","yes")
        intel_active = row.get("HS_company_intel_active", "false").strip().lower() in ("true","1","yes")
        if not active and not intel_active:
            continue
        people.append({
            "name":         row.get("name", "").strip(),
            "email":        row.get("email", "").strip().lower(),
            "active":       active,
            "intel_active": intel_active
        })
    print(f"  ✅  Loaded {len(people)} active salespeople")
    return people


def load_prompt(service, filename):
    file_id = drive_find_file(service, filename, GDRIVE_PROMPTS_FOLDER_ID)
    if not file_id:
        print(f"  ⚠️  Prompt not found: {filename}")
        return None
    return drive_read_text(service, file_id)


def load_processed(service):
    file_id = drive_find_file(service, PROCESSED_FILENAME, GDRIVE_SRC_FOLDER_ID)
    if not file_id:
        return {}, None
    try:
        data = drive_read_text(service, file_id)
        return json.loads(data), file_id
    except:
        return {}, file_id


def save_processed(service, processed, file_id):
    data = json.dumps(processed, indent=2)
    new_id = drive_write_text(service, PROCESSED_FILENAME, data,
                               GDRIVE_SRC_FOLDER_ID, existing_file_id=file_id)
    return new_id


def append_review_csv(service, row, existing_file_id):
    """Append one row to management_review.csv in src/."""
    fieldnames = ["rep_name", "meeting_date", "meeting_time", "company",
                  "customer_name", "hs_meeting_id", "transcript_id",
                  "match_confidence", "grade", "deal_stage"]
    # Try to read existing
    existing_content = ""
    if existing_file_id:
        try:
            existing_content = drive_read_text(service, existing_file_id)
        except:
            existing_content = ""

    buf = io.StringIO(existing_content)
    has_header = existing_content.startswith("rep_name")

    out = io.StringIO()
    if existing_content:
        out.write(existing_content.rstrip("\n") + "\n")
    else:
        writer = csv.DictWriter(out, fieldnames=fieldnames)
        writer.writeheader()

    writer = csv.DictWriter(out, fieldnames=fieldnames, extrasaction="ignore")
    writer.writerow(row)

    new_id = drive_write_text(service, REVIEW_CSV_FILENAME, out.getvalue(),
                               GDRIVE_SRC_FOLDER_ID, existing_file_id=existing_file_id)
    return new_id


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def section(title):
    print()
    print("=" * 70)
    print(f"  {title}")
    print("=" * 70)


def slack_notify(webhook_url, message):
    if not webhook_url or IS_BACKFILL:
        return
    try:
        requests.post(webhook_url, json={"text": message}, timeout=10)
    except:
        pass


def parse_dt(iso_str):
    if not iso_str:
        return None
    try:
        return datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    except:
        return None


def duration_mins(start_str, end_str):
    s = parse_dt(start_str)
    e = parse_dt(end_str)
    if s and e:
        return int((e - s).total_seconds() / 60)
    return -1


def confidence_score(delta_mins):
    if delta_mins <= HIGH_THRESHOLD:
        return (3, "HIGH")
    elif delta_mins <= MATCH_WINDOW_MINS:
        return (2, "MEDIUM")
    elif delta_mins <= 30:
        return (1, "LOW")
    else:
        return (0, "NONE")


def safe_filename(s):
    return "".join(c if c.isalnum() or c in "._- " else "_" for c in s).strip()


def extract_grade(audit_text):
    m = re.search(r'GRADE:\s*([1-5])', audit_text[-500:])
    return int(m.group(1)) if m else None


def extract_competitors(hs_summary):
    import re
    m = re.search(r'COMPETITORS MENTIONED\s*\n(.*?)(?:\n[A-Z ]{3,}\n|$)',
                  hs_summary, re.DOTALL)
    if m:
        block = m.group(1).strip()
        if block and "none" not in block.lower():
            return block
    return None


def extract_deal_health(hs_summary):
    import re
    m = re.search(r'DEAL HEALTH\s*\n.*?(\d)/5', hs_summary[:1000], re.DOTALL)
    if m:
        return m.group(1)
    return None


def get_company_contacts(hs_key, company_id):
    headers = {"Authorization": f"Bearer {hs_key}"}
    try:
        res = requests.get(
            f"{HS_BASE_URL}/crm/v3/objects/companies/{company_id}/associations/contacts",
            headers=headers, timeout=10)
        if res.status_code != 200:
            return []
        contact_ids = [r["id"] for r in res.json().get("results", [])]
        contacts = []
        for cid in contact_ids[:10]:
            cr = requests.get(
                f"{HS_BASE_URL}/crm/v3/objects/contacts/{cid}"
                f"?properties=firstname,lastname,email,jobtitle,num_contacted_notes",
                headers=headers, timeout=10)
            if cr.status_code == 200:
                contacts.append(cr.json())
        return contacts
    except:
        return []


def get_company_deals(hs_key, company_id):
    headers = {"Authorization": f"Bearer {hs_key}"}
    try:
        res = requests.get(
            f"{HS_BASE_URL}/crm/v3/objects/companies/{company_id}/associations/deals",
            headers=headers, timeout=10)
        if res.status_code != 200:
            return []
        deal_ids = [r["id"] for r in res.json().get("results", [])]
        deals = []
        for did in deal_ids[:5]:
            dr = requests.get(
                f"{HS_BASE_URL}/crm/v3/objects/deals/{did}"
                f"?properties=dealname,dealstage,amount,closedate",
                headers=headers, timeout=10)
            if dr.status_code == 200:
                deals.append(dr.json())
        return deals
    except:
        return []


def run_company_intel(rep_name, meetings, hs_key, gemini_key, slack_intel,
                      prompt_intel, drive_service):
    if not meetings:
        return
    seen_companies = set()
    for m in meetings[:5]:
        _details = get_meeting_details(hs_key, m["hs_id"]) if m.get("hs_id") else {}
        contact_id = _details.get("contact_id") if _details else None
        customer_info = _details if _details else None
        if not customer_info:
            continue
        company_name = customer_info.get("company_name", "") if isinstance(customer_info, dict) else ""
        company_id   = customer_info.get("company_id", "") if isinstance(customer_info, dict) else ""
        if not company_name or company_name in seen_companies:
            continue
        seen_companies.add(company_name)

        # Skip if intel file already generated today
        date_str_intel = datetime.now().strftime("%Y-%m-%d")
        safe_rep_intel  = safe_filename(rep_name.split()[0])
        safe_co_intel   = safe_filename(company_name.replace(" ", "_"))[:40]
        intel_filename  = f"{date_str_intel}_{safe_rep_intel}_{safe_co_intel}-Intel.txt"
        rep_folder_check = drive_get_or_create_folder(
            drive_service, safe_filename(rep_name), GDRIVE_INTEL_FOLDER_ID)
        existing = drive_find_file(drive_service, intel_filename, rep_folder_check)
        if existing:
            print(f"  [Intel] Skipping {company_name} — already generated today")
            continue
        all_contacts = get_company_contacts(hs_key, company_id) if company_id else []
        deals        = get_company_deals(hs_key, company_id) if company_id else []
        hs_context   = f"Contact: {customer_info.get('customer_name','?')} | {customer_info.get('customer_title','?')}"
        hs_context  += f"\nCompany: {company_name}"
        if deals:
            for d in deals[:3]:
                p = d.get("properties", {})
                hs_context += f"\nDeal: {p.get('dealname','?')} | Stage: {p.get('dealstage','?')} | Amount: {p.get('amount','?')}"
        if all_contacts:
            hs_context += f"\nOther contacts at company: {len(all_contacts)}"
            for c in all_contacts[:3]:
                p = c.get("properties", {})
                cname = f"{p.get('firstname','')} {p.get('lastname','')}".strip()
                hs_context += f"\n  - {cname} | {p.get('jobtitle','?')} | contacted {p.get('num_contacted_notes','0')}x"
        print(f"  [Intel] Running for {company_name}...")
        brief = gemini_call(f"{prompt_intel}\n\nHubSpot and context data:\n{hs_context}", gemini_key)
        ok = not brief.startswith(("ERROR", "EXCEPTION"))
        print(f"  [Intel] {'OK' if ok else 'FAIL'} {company_name}")
        if ok:
            date_str   = datetime.now().strftime("%Y-%m-%d")
            safe_co    = safe_filename(company_name.replace(" ", "_"))[:40]
            safe_rep   = safe_filename(rep_name.split()[0])
            filename   = f"{date_str}_{safe_rep}_{safe_co}-Intel.txt"
            rep_folder = drive_get_or_create_folder(
                drive_service, safe_filename(rep_name), GDRIVE_INTEL_FOLDER_ID)
            drive_write_text(drive_service, filename,
                f"COMPANY INTEL BRIEF\n{'='*60}\nCompany: {company_name}\nRep: {rep_name}\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n{'='*60}\n\n{brief}",
                rep_folder)
            if slack_intel:
                try:
                    requests.post(slack_intel, json={"text": f"🔍 *Company Intel* — {rep_name}\n*{company_name}*\nFile: `{filename}`"}, timeout=10)
                except:
                    pass



def extract_call_type_value(hs_summary):
    """Map call type text to dropdown value for custom object."""
    text = hs_summary[:500].lower()
    if "discovery" in text or "intro" in text:
        return "discovery_demo"
    if "demo" in text:
        return "discovery_demo"
    if "follow" in text:
        return "follow_up"
    if "proposal" in text or "pricing" in text:
        return "proposal_pricing"
    if "closing" in text:
        return "closing"
    if "exploratory" in text:
        return "exploratory"
    return "other"


def extract_event_format(hs_summary):
    text = hs_summary.lower()
    if "in-person" in text or "in person" in text:
        return "in_person"
    if "hybrid" in text:
        return "hybrid"
    if "virtual" in text or "online" in text or "zoom" in text or "teams" in text:
        return "virtual"
    return "not_stated"


def extract_platform(hs_summary):
    text = hs_summary.lower()
    if "teams" in text:
        return "teams"
    if "zoom" in text:
        return "zoom"
    if "google meet" in text:
        return "google_meet"
    if "webex" in text:
        return "webex"
    if "goto" in text or "go to" in text:
        return "goto"
    return "not_stated"


def extract_urgency(hs_summary):
    text = hs_summary.lower()
    if "tomorrow" in text or "today" in text:
        return "tomorrow"
    if "this week" in text:
        return "this_week"
    if "this month" in text:
        return "this_month"
    if "this quarter" in text or "q1" in text or "q2" in text or "q3" in text or "q4" in text:
        return "this_quarter"
    return "not_stated"


def extract_call_type_value(hs_summary):
    text = hs_summary[:500].lower()
    if "follow" in text:
        return "follow_up"
    if "proposal" in text or "pricing" in text:
        return "proposal_pricing"
    if "closing" in text:
        return "closing"
    if "exploratory" in text:
        return "exploratory"
    if "discovery" in text or "demo" in text or "intro" in text:
        return "discovery_demo"
    return "other"


def extract_event_format(hs_summary):
    text = hs_summary.lower()
    if "hybrid" in text:
        return "hybrid"
    if "in-person" in text or "in person" in text:
        return "in_person"
    if "virtual" in text or "zoom" in text or "teams" in text or "online" in text:
        return "virtual"
    return "not_stated"


def extract_platform(hs_summary):
    text = hs_summary.lower()
    if "teams" in text:
        return "teams"
    if "zoom" in text:
        return "zoom"
    if "google meet" in text:
        return "google_meet"
    if "webex" in text:
        return "webex"
    if "goto" in text:
        return "goto"
    return "not_stated"


def extract_urgency(hs_summary):
    text = hs_summary.lower()
    if "tomorrow" in text or "today" in text:
        return "tomorrow"
    if "this week" in text:
        return "this_week"
    if "this month" in text:
        return "this_month"
    if "this quarter" in text or "q1" in text or "q2" in text or "q3" in text or "q4" in text:
        return "this_quarter"
    return "not_stated"


def extract_call_type_abbrev(hs_summary):
    """Extract call type from HS summary and return short code."""
    mapping = {
        "Discovery": "DISC", "Intro": "DISC",
        "Follow-Up": "FOLLOWUP", "Follow Up": "FOLLOWUP",
        "Demo": "DEMO",
        "Proposal": "PROPOSAL", "Pricing": "PROPOSAL",
        "Closing": "CLOSING",
        "Other": "OTHER"
    }
    for key, abbrev in mapping.items():
        if key.lower() in hs_summary[:500].lower():
            return abbrev
    return "CALL"


CONF_ICON = {"HIGH": "✅", "MEDIUM": "🟡", "LOW": "🟠", "NONE": "❌"}


# ---------------------------------------------------------------------------
# TRANSCRIPT DOWNLOAD
# ---------------------------------------------------------------------------

def download_transcript(t_id, wordly_key):
    url    = f"{WORDLY_BASE_URL}/transcripts/{t_id}/original?format=txt&speaker_names=true"
    chunks = []
    try:
        with requests.get(url, headers={"x-wordly-api-key": wordly_key},
                          timeout=30, stream=True) as res:
            if res.status_code != 200:
                return None, f"http_{res.status_code}"
            try:
                for chunk in res.iter_content(chunk_size=1024, decode_unicode=True):
                    if chunk:
                        chunks.append(chunk)
            except requests.exceptions.ChunkedEncodingError:
                pass
        text = "".join(chunks)
        return (text, "ok") if text.strip() else (None, "empty")
    except Exception as e:
        return None, f"exception: {e}"


# ---------------------------------------------------------------------------
# HUBSPOT
# ---------------------------------------------------------------------------

def fetch_all_owners(hs_key):
    headers = {"Authorization": f"Bearer {hs_key}", "Content-Type": "application/json"}
    try:
        res = requests.get(f"{HS_BASE_URL}/crm/v3/owners?limit=100",
                           headers=headers, timeout=10)
        if res.status_code == 200:
            return res.json().get("results", [])
    except:
        pass
    return []


def resolve_owner(email, all_owners):
    for o in all_owners:
        if o.get("email", "").lower() == email.lower():
            return o
    return None


def pull_hs_meetings(hs_key, owner_id, lookback_hours=None, lookback_days=None):
    headers   = {"Authorization": f"Bearer {hs_key}", "Content-Type": "application/json"}
    now_utc   = datetime.now(timezone.utc)
    since_utc = now_utc - (timedelta(hours=lookback_hours) if lookback_hours
                           else timedelta(days=lookback_days or 10))
    payload = {
        "filterGroups": [{
            "filters": [
                {"propertyName": "hubspot_owner_id",      "operator": "EQ",  "value": str(owner_id)},
                {"propertyName": "hs_meeting_start_time", "operator": "GTE", "value": str(int(since_utc.timestamp()*1000))},
                {"propertyName": "hs_meeting_start_time", "operator": "LTE", "value": str(int(now_utc.timestamp()*1000))}
            ]
        }],
        "properties": ["hs_meeting_title", "hs_meeting_start_time",
                       "hs_meeting_end_time", "hs_meeting_outcome", "hubspot_owner_id"],
        "sorts":  [{"propertyName": "hs_meeting_start_time", "direction": "ASCENDING"}],
        "limit":  100
    }
    try:
        res = requests.post(f"{HS_BASE_URL}/crm/v3/objects/meetings/search",
                            headers=headers, json=payload, timeout=15)
        if res.status_code != 200:
            return []
        meetings = []
        for m in res.json().get("results", []):
            props = m.get("properties", {})
            start = props.get("hs_meeting_start_time", "")
            end   = props.get("hs_meeting_end_time", "")
            meetings.append({
                "hs_id":     m.get("id"),
                "title":     props.get("hs_meeting_title") or "Untitled",
                "start":     parse_dt(start),
                "end":       parse_dt(end),
                "duration":  duration_mins(start, end),
                "outcome":   props.get("hs_meeting_outcome") or "—",
                "start_str": start
            })
        return meetings
    except:
        return []


def get_meeting_details(hs_key, meeting_id):
    """
    Fetch contact, company, and deal info for a meeting.
    Returns dict with contact_id, customer_name, company_name, company_id,
    deal_id, deal_name, deal_stage.
    """
    headers = {"Authorization": f"Bearer {hs_key}", "Content-Type": "application/json"}
    result  = {
        "contact_id":    None,
        "customer_name": "Unknown Customer",
        "customer_email": "",
        "customer_title": "",
        "company_id":    None,
        "company_name":  "Unknown Company",
        "deal_id":       None,
        "deal_name":     None,
        "deal_stage":    None
    }

    try:
        # Get associations — contacts, companies, deals in one call
        res = requests.get(
            f"{HS_BASE_URL}/crm/v3/objects/meetings/{meeting_id}"
            f"?associations=contacts,companies,deals",
            headers=headers, timeout=10)
        if res.status_code != 200:
            return result
        data         = res.json()
        associations = data.get("associations", {})

        # Contact
        contacts = associations.get("contacts", {}).get("results", [])
        if contacts:
            contact_id = contacts[0]["id"]
            result["contact_id"] = contact_id
            cr = requests.get(
                f"{HS_BASE_URL}/crm/v3/objects/contacts/{contact_id}"
                f"?properties=firstname,lastname,email,jobtitle",
                headers=headers, timeout=10)
            if cr.status_code == 200:
                p = cr.json().get("properties", {})
                name = f"{p.get('firstname','') or ''} {p.get('lastname','') or ''}".strip()
                result["customer_name"]  = name or p.get("email", "Unknown")
                result["customer_email"] = p.get("email", "")
                result["customer_title"] = p.get("jobtitle", "")

        # Company
        companies = associations.get("companies", {}).get("results", [])
        if companies:
            company_id = companies[0]["id"]
            result["company_id"] = company_id
            cr = requests.get(
                f"{HS_BASE_URL}/crm/v3/objects/companies/{company_id}"
                f"?properties=name,domain",
                headers=headers, timeout=10)
            if cr.status_code == 200:
                result["company_name"] = cr.json().get("properties", {}).get("name", "Unknown Company")

        # Deal
        deals = associations.get("deals", {}).get("results", [])
        if deals:
            deal_id = deals[0]["id"]
            result["deal_id"] = deal_id
            dr = requests.get(
                f"{HS_BASE_URL}/crm/v3/objects/deals/{deal_id}"
                f"?properties=dealname,dealstage",
                headers=headers, timeout=10)
            if dr.status_code == 200:
                p = dr.json().get("properties", {})
                result["deal_name"]  = p.get("dealname")
                result["deal_stage"] = p.get("dealstage")

    except Exception as e:
        print(f"    ⚠️  Meeting details error: {e}")

    return result


def get_all_meeting_contacts(hs_key, meeting_id):
    """Return list of all contact IDs associated with a HubSpot meeting."""
    headers = {"Authorization": f"Bearer {hs_key}"}
    try:
        res = requests.get(
            f"{HS_BASE_URL}/crm/v3/objects/meetings/{meeting_id}/associations/contacts",
            headers=headers, timeout=10)
        if res.status_code == 200:
            return [r["id"] for r in res.json().get("results", [])]
    except:
        pass
    return []


def extract_wcs_fields(hs_summary, gemini_key):
    """Use Gemini to extract structured fields from the HS summary text."""
    prompt = """Extract structured data from this sales call summary.
Return ONLY a JSON object with these exact keys. No markdown, no explanation.
If a field was not mentioned, use null.

{
  "call_type": "one of: discovery_demo | follow_up | proposal_pricing | closing | exploratory | other",
  "deal_health": "integer 1-5 or null",
  "deal_health_reason": "one line reason or null",
  "competitors_mentioned": "comma-separated names or null",
  "deal_urgency": "one of: tomorrow | this_week | this_month | this_quarter | this_year | next_year | other | not_stated",
  "event_format_wcs": "one of: in_person | virtual | hybrid | not_stated",
  "platform_wcs": "one of: teams | zoom | google_meet | webex | goto | in_person_only | other | not_stated",
  "languages_needed": "text or null",
  "audience_size": "integer or null",
  "hours_needed": "integer or null",
  "customer_type_wcs": "one of: direct | partner | reseller",
  "internal_referral": "true or false",
  "pain_points": "brief bullet list or null",
  "next_steps": "brief bullet list or null",
  "what_brought_them": "one or two sentences or null",
  "event_date_wcs": "YYYY-MM-DD or null"
}

SUMMARY:
""" + hs_summary[:3000]

    try:
        result = gemini_call(prompt, gemini_key)
        # Strip markdown fences if present
        result = result.strip()
        if result.startswith("```"):
            result = re.sub(r"```(?:json)?", "", result).strip().rstrip("```").strip()
        import json
        return json.loads(result)
    except Exception as e:
        print(f"    ⚠️  Field extraction failed: {e}")
        return {}


def write_wcs_record(hs_key, fields, contact_id, company_id=None):
    """Create a Wordly Call Summary custom object record and associate it."""
    headers = {"Authorization": f"Bearer {hs_key}", "Content-Type": "application/json"}

    # Convert date strings to midnight UTC milliseconds
    import datetime as dt_module
    props = {}
    date_fields = {"call_date", "event_date_wcs"}
    for k, v in fields.items():
        if v is None:
            continue
        if k in date_fields and isinstance(v, str) and len(v) == 10:
            try:
                d = dt_module.datetime.strptime(v, "%Y-%m-%d")
                props[k] = int(d.replace(tzinfo=dt_module.timezone.utc).timestamp() * 1000)
            except:
                props[k] = v
        elif isinstance(v, bool):
            props[k] = str(v).lower()
        else:
            props[k] = v

    # Remove record_name — not a valid HS property
    props.pop("record_name", None)

    res = requests.post(
        f"{HS_BASE_URL}/crm/v3/objects/{WCS_OBJECT_TYPE_ID}",
        headers=headers,
        json={"properties": props},
        timeout=15
    )
    if res.status_code not in (200, 201):
        print(f"    ⚠️  WCS record failed: {res.status_code} {res.text[:200]}")
        return None

    record_id = res.json().get("id")

    # Associate to contact
    if contact_id:
        requests.put(
            f"{HS_BASE_URL}/crm/v4/objects/{WCS_OBJECT_TYPE_ID}/{record_id}"
            f"/associations/default/contacts/{contact_id}",
            headers=headers, timeout=10)

    # Associate to company
    if company_id:
        requests.put(
            f"{HS_BASE_URL}/crm/v4/objects/{WCS_OBJECT_TYPE_ID}/{record_id}"
            f"/associations/default/companies/{company_id}",
            headers=headers, timeout=10)

    return record_id


def get_all_meeting_contacts(hs_key, meeting_id):
    """Return list of all contact IDs associated with a HubSpot meeting."""
    headers = {"Authorization": f"Bearer {hs_key}"}
    try:
        res = requests.get(
            f"{HS_BASE_URL}/crm/v3/objects/meetings/{meeting_id}/associations/contacts",
            headers=headers, timeout=10)
        if res.status_code == 200:
            return [r["id"] for r in res.json().get("results", [])]
    except:
        pass
    return []


def extract_wcs_fields(hs_summary, gemini_key):
    """Use Gemini to extract structured fields from the HS summary text."""
    prompt = """Extract structured data from this sales call summary.
Return ONLY a JSON object with these exact keys. No markdown, no explanation.
If a field was not mentioned, use null.

{
  "call_type": "one of: discovery_demo | follow_up | proposal_pricing | closing | exploratory | other",
  "deal_health": "integer 1-5 or null",
  "deal_health_reason": "one line reason or null",
  "competitors_mentioned": "comma-separated names or null",
  "deal_urgency": "one of: tomorrow | this_week | this_month | this_quarter | this_year | next_year | other | not_stated",
  "event_format_wcs": "one of: in_person | virtual | hybrid | not_stated",
  "platform_wcs": "one of: teams | zoom | google_meet | webex | goto | in_person_only | other | not_stated",
  "languages_needed": "text or null",
  "audience_size": "integer or null",
  "hours_needed": "integer or null",
  "customer_type_wcs": "one of: direct | partner | reseller",
  "internal_referral": "true or false",
  "pain_points": "brief bullet list or null",
  "next_steps": "brief bullet list or null",
  "what_brought_them": "one or two sentences or null",
  "event_date_wcs": "YYYY-MM-DD or null"
}

SUMMARY:
""" + hs_summary[:3000]

    try:
        result = gemini_call(prompt, gemini_key)
        # Strip markdown fences if present
        result = result.strip()
        if result.startswith("```"):
            result = re.sub(r"```(?:json)?", "", result).strip().rstrip("```").strip()
        import json
        return json.loads(result)
    except Exception as e:
        print(f"    ⚠️  Field extraction failed: {e}")
        return {}


def write_wcs_record(hs_key, fields, contact_id, company_id=None):
    """Create a Wordly Call Summary custom object record and associate it."""
    import datetime as dt_module
    headers = {"Authorization": f"Bearer {hs_key}", "Content-Type": "application/json"}
    props = {}
    date_fields = {"call_date", "event_date_wcs"}
    for k, v in fields.items():
        if v is None:
            continue
        if k in date_fields and isinstance(v, str) and len(v) == 10:
            try:
                d = dt_module.datetime.strptime(v, "%Y-%m-%d")
                props[k] = int(d.replace(tzinfo=dt_module.timezone.utc).timestamp() * 1000)
            except:
                props[k] = v
        elif isinstance(v, bool):
            props[k] = str(v).lower()
        else:
            props[k] = v

    # Remove record_name — not a valid HS property
    props.pop("record_name", None)

    res = requests.post(
        f"{HS_BASE_URL}/crm/v3/objects/{WCS_OBJECT_TYPE_ID}",
        headers=headers,
        json={"properties": props},
        timeout=15
    )
    if res.status_code not in (200, 201):
        print(f"    WCS record failed: {res.status_code} {res.text[:200]}")
        return None

    record_id = res.json().get("id")

    if contact_id:
        requests.put(
            f"{HS_BASE_URL}/crm/v4/objects/{WCS_OBJECT_TYPE_ID}/{record_id}"
            f"/associations/default/contacts/{contact_id}",
            headers=headers, timeout=10)

    if company_id:
        requests.put(
            f"{HS_BASE_URL}/crm/v4/objects/{WCS_OBJECT_TYPE_ID}/{record_id}"
            f"/associations/default/companies/{company_id}",
            headers=headers, timeout=10)

    return record_id


def write_hs_note(hs_key, contact_id, note_body, company_id=None):
    headers = {"Authorization": f"Bearer {hs_key}", "Content-Type": "application/json"}
    associations = [{
        "to":    {"id": contact_id},
        "types": [{"associationCategory": "HUBSPOT_DEFINED",
                   "associationTypeId": 202}]
    }]
    if company_id:
        associations.append({
            "to":    {"id": company_id},
            "types": [{"associationCategory": "HUBSPOT_DEFINED",
                       "associationTypeId": 190}]
        })
    payload = {
        "properties": {
            "hs_note_body": note_body,
            "hs_timestamp": str(int(datetime.now().timestamp() * 1000)),
        },
        "associations": associations
    }
    try:
        res = requests.post(f"{HS_BASE_URL}/crm/v3/objects/notes",
                            headers=headers, json=payload, timeout=15)
        if res.status_code in (200, 201):
            return res.json().get("id")
        print(f"    ⚠️  HS note write failed: {res.status_code}")
        return None
    except Exception as e:
        print(f"    ⚠️  HS note exception: {e}")
        return None


# ---------------------------------------------------------------------------
# WORDLY
# ---------------------------------------------------------------------------

def get_session_state(session_id, wordly_key):
    try:
        res = requests.get(
            f"{WORDLY_BASE_URL}/sessions/{session_id}",
            headers={"x-wordly-api-key": wordly_key}, timeout=10)
        if res.status_code == 200:
            return res.json().get("state", "unknown")
        if res.status_code == 404:
            return "ended"  # session exists in transcripts list, treat as complete
        return f"error_{res.status_code}"
    except:
        return "exception"


def pull_wordly_transcripts(wordly_key, lookback_hours=None, lookback_days=None):
    now_utc   = datetime.now(timezone.utc)
    since_utc = now_utc - (timedelta(hours=lookback_hours) if lookback_hours
                           else timedelta(days=lookback_days or 10))
    try:
        res = requests.get(
            f"{WORDLY_BASE_URL}/transcripts?page=1&limit=100",
            headers={"x-wordly-api-key": wordly_key}, timeout=15)
        if res.status_code == 401:
            return None
        if res.status_code != 200:
            return []
        transcripts = []
        for t in res.json().get("transcripts", []):
            start = parse_dt(t.get("startTime"))
            if not start or start < since_utc:
                continue
            sid = t.get("sessionId", "?")
            dur = duration_mins(t.get("startTime",""), t.get("endTime",""))
            if 0 <= dur < MIN_DURATION_MINS:
                continue
            if not t.get("endTime"):
                continue  # session still active — catch next run
            transcripts.append({
                "transcript_id": t.get("transcriptId"),
                "session_id":    sid,
                "title":         t.get("title", "?"),
                "start":         start,
                "end":           parse_dt(t.get("endTime")),
                "duration":      dur,
                "start_str":     t.get("startTime","")
            })
        return transcripts
    except Exception as e:
        print(f"    ❌  Exception: {e}")
        return []


# ---------------------------------------------------------------------------
# MATCHING
# ---------------------------------------------------------------------------

def match(transcripts, meetings):
    candidates = []
    for t in transcripts:
        for m in meetings:
            if not t["start"] or not m["start"]:
                continue
            delta = abs((t["start"] - m["start"]).total_seconds() / 60)
            score, conf = confidence_score(delta)
            if score > 0:
                candidates.append({
                    "transcript": t, "meeting": m,
                    "delta_mins": round(delta, 1),
                    "score": score, "confidence": conf
                })
    candidates.sort(key=lambda x: (-x["score"], x["delta_mins"]))
    used_t = set()
    used_m = set()
    assigned = []
    for c in candidates:
        t_id = c["transcript"]["transcript_id"]
        m_id = c["meeting"]["hs_id"]
        if t_id not in used_t and m_id not in used_m:
            assigned.append(c)
            used_t.add(t_id)
            used_m.add(m_id)
    for t in transcripts:
        if t["transcript_id"] not in used_t:
            assigned.append({
                "transcript": t, "meeting": None,
                "delta_mins": None, "score": 0, "confidence": "NONE"
            })
    assigned.sort(key=lambda x: x["transcript"]["start"] or
                  datetime.min.replace(tzinfo=timezone.utc))
    return assigned


# ---------------------------------------------------------------------------
# GEMINI
# ---------------------------------------------------------------------------

def gemini_call(prompt_text, gemini_key, retries=2):
    for attempt in range(retries):
        try:
            res = requests.post(
                f"{GEMINI_URL}?key={gemini_key}",
                json={"contents": [{"parts": [{"text": prompt_text}]}]},
                timeout=90)
            if res.status_code == 200:
                return res.json()["candidates"][0]["content"]["parts"][0]["text"]
            return f"ERROR {res.status_code}: {res.text[:200]}"
        except requests.exceptions.Timeout:
            if attempt < retries - 1:
                time.sleep(5)
                continue
            return "EXCEPTION: Gemini timed out."
        except Exception as e:
            return f"EXCEPTION: {e}"
    return "EXCEPTION: All retries exhausted."


def get_customer_name_from_transcript(text, gemini_key):
    prompt = (
        "Read the following sales call transcript excerpt. "
        "Identify the name of the CUSTOMER (not the salesperson from Wordly). "
        "Reply with ONLY the customer's name. "
        "If you cannot determine it, reply with: Unknown\n\n"
        f"TRANSCRIPT:\n{text[:3000]}"
    )
    result = gemini_call(prompt, gemini_key).strip()
    return result if result and "Unknown" not in result else None


# ---------------------------------------------------------------------------
# SUMMARIZE ONE MATCH
# ---------------------------------------------------------------------------

def summarize_match(r, person_name, hs_key, gemini_key,
                    prompt_hs, prompt_mgmt, prompt_competitive, prompt_roadmap,
                    processed, processed_file_id, review_csv_file_id,
                    drive_service, slack_url):
    t    = r["transcript"]
    m    = r["meeting"]
    t_id = t["transcript_id"]
    conf = r["confidence"]

    if t_id in processed:
        print(f"    ⏭️   Already processed: {t_id}")
        return None, processed_file_id, review_csv_file_id

    wordly_key = r.get("wordly_key")
    if not wordly_key:
        print(f"    ❌  No Wordly key for {person_name}")
        return None, processed_file_id, review_csv_file_id

    text, status = download_transcript(t_id, wordly_key)
    if not text:
        print(f"    ⚠️  Download failed: {status}")
        return None, processed_file_id, review_csv_file_id

    # Check for merged session (restart detected)
    merged = r.get("_merged")
    session_restart_note = ""
    if merged:
        t2_id = merged["transcript"]["transcript_id"]
        text2, status2 = download_transcript(t2_id, wordly_key)
        if text2:
            # Concatenate in chronological order
            if merged["transcript"]["start"] < t["start"]:
                text = text2 + "\n\n[--- SESSION RESTART ---]\n\n" + text
            else:
                text = text + "\n\n[--- SESSION RESTART ---]\n\n" + text2
            session_restart_note = "Note: Summary covers 2 Wordly sessions from the same meeting (session restart detected).\n"
            print(f"    ⚠️  Session restart detected — transcripts merged")
            # Mark second transcript as processed too
            processed[t2_id] = {
                "processed_at": datetime.now(timezone.utc).isoformat(),
                "rep": person_name, "company": "merged", "date": date_str, "grade": "merged"
            }

    m_hs_id   = m["hs_id"] if m else None
    m_start   = m["start_str"] if m else t["start_str"]
    dt        = parse_dt(m_start) or parse_dt(t["start_str"])
    date_str  = dt.strftime("%Y-%m-%d") if dt else "unknown"
    time_str  = dt.strftime("%H%M") if dt else "0000"
    date_disp = dt.strftime("%B %d, %Y") if dt else "Unknown Date"
    time_disp = dt.strftime("%I:%M %p UTC") if dt else ""

    # Get meeting details from HubSpot
    details       = get_meeting_details(hs_key, m_hs_id) if m_hs_id else {}
    contact_id    = details.get("contact_id")
    customer_name = details.get("customer_name", "Unknown Customer")
    company_name  = details.get("company_name", "Unknown Company")
    deal_stage    = details.get("deal_stage")
    deal_name     = details.get("deal_name")

    if customer_name == "Unknown Customer":
        customer_name = get_customer_name_from_transcript(text, gemini_key) or "Unknown Customer"

    print(f"    Customer: {customer_name} | Company: {company_name}"
          + (f" | Deal: {deal_stage}" if deal_stage else ""))

    # Build filename base using company name
    safe_company = safe_filename((company_name or "Unknown").replace(" ", "_"))[:30]
    base         = f"{date_str}_{time_str}_{safe_company}"

    # Drive folder structure — separate folders per output type
    safe_rep          = safe_filename(person_name)
    hs_person_folder  = drive_get_or_create_folder(drive_service, safe_rep, GDRIVE_HS_FOLDER_ID)
    aud_person_folder = drive_get_or_create_folder(drive_service, safe_rep, GDRIVE_AUDITS_FOLDER_ID)
    tx_person_folder  = drive_get_or_create_folder(drive_service, safe_rep, GDRIVE_TRANSCRIPTS_FOLDER_ID)

    # Filename base: YYYY-MM-DD_RepFirstName_CompanyShort
    rep_first  = person_name.split()[0]
    file_base  = f"{date_str}_{rep_first}_{safe_company}"

    saved_ids = {}
    def save_file(folder_id, suffix, body, header):
        filename = f"{file_base}-{suffix}.txt"
        body_out = header + "\n" + "="*60 + "\n\n" + body
        saved_ids[suffix] = drive_write_text(drive_service, filename, body_out, folder_id)
        return filename

    # Save raw transcript to Transcripts/RepName/
    save_file(tx_person_folder, "Transcript", text,
              f"RAW TRANSCRIPT\nSalesperson: {person_name}\n"
              f"Customer: {customer_name} | Company: {company_name}\n"
              f"Date: {date_str} {time_str}")
    transcript_link = (f"https://drive.google.com/file/d/{saved_ids['Transcript']}/view"
                       if saved_ids.get("Transcript") else "")

    # Build context for all prompts
    deal_context = ""
    if deal_stage:
        deal_context = f"\nDeal: {deal_name or 'unnamed'} | Stage: {deal_stage}"

    call_context = (
        f"Salesperson: {person_name}\n"
        f"Customer: {customer_name}"
        + (f" | {details.get('customer_title','')}" if details.get('customer_title') else "")
        + f"\nCompany: {company_name}"
        + deal_context
        + f"\nMeeting: {m['title'] if m else 'Unmatched'}"
        + f"\nDate: {date_disp} {time_disp}\n\n"
        f"TRANSCRIPT:\n{text}"
    )

    # --- HubSpot Summary ---
    hs_header = f"AI Summary — {person_name} & {customer_name} ({company_name}) | {date_disp} {time_disp}"
    print(f"    Generating HubSpot summary...", end=" ", flush=True)
    hs_summary = gemini_call(f"{prompt_hs}\n\n{call_context}", gemini_key)
    ok_hs = not hs_summary.startswith(("ERROR", "EXCEPTION"))
    print("✅" if ok_hs else "❌")

    # Call type extracted but not used in filename (kept for review CSV)
    call_type = extract_call_type_abbrev(hs_summary) if ok_hs else "CALL"

    competitors = extract_competitors(hs_summary)
    deal_health = extract_deal_health(hs_summary)
    signal_lines = ""
    if competitors:
        signal_lines += f"\nCompetitors Mentioned\n{competitors}"
    if deal_health:
        signal_lines += f"\n\nOverall deal health estimate\n{deal_health}/5"
    hs_note_body = f"{hs_header}\n{session_restart_note}{signal_lines}\n\n{hs_summary}"
    hs_note_body = (hs_note_body
        .replace("**", "")
        .replace("## ", "")
        .replace("# ", "")
        .replace("* ", "- ")
        .replace("*	", "- "))
    save_file(hs_person_folder, "HS", hs_note_body,
              f"HUBSPOT SUMMARY\nSalesperson: {person_name}\n"
              f"Customer: {customer_name} | Company: {company_name}\n"
              f"Date: {m_start} | HS ID: {m_hs_id or 'UNMATCHED'}")

    note_id = None
    wcs_id  = None
    time.sleep(2)

    # --- Sales Audit ---
    print(f"    Generating sales audit...", end=" ", flush=True)
    mgmt_summary = gemini_call(f"{prompt_mgmt}\n\n{call_context}", gemini_key)
    ok_audit     = not mgmt_summary.startswith(("ERROR", "EXCEPTION"))
    print("✅" if ok_audit else "❌")
    grade     = extract_grade(mgmt_summary) if ok_audit else None
    grade_str = str(grade) if grade else "X"
    print(f"    Grade: {grade_str}/5")
    save_file(aud_person_folder, f"Audit_G{grade_str}", mgmt_summary,
              f"SALES AUDIT — GRADE {grade_str}/5\n"
              f"Salesperson: {person_name} | Customer: {customer_name}\n"
              f"Company: {company_name} | Date: {m_start}"
              + (f"\nTranscript: {transcript_link}" if transcript_link else ""))
    time.sleep(2)

    # Write WCS record after audit so grade is available
    if contact_id and ok_hs:
        nd = "Not Discussed"
        print(f"    Extracting structured fields...", end=" ", flush=True)
        extracted = extract_wcs_fields(hs_summary, gemini_key) if ok_hs else {}
        print("OK" if extracted else "used defaults")
        # Force all extracted values to scalar — Gemini sometimes returns lists
        for k in list(extracted.keys()):
            v = extracted[k]
            if isinstance(v, list):
                extracted[k] = ", ".join(str(i) for i in v) if v else None
            elif isinstance(v, dict):
                extracted[k] = str(v)

        # Record name: date + rep + company
        call_title = f"{person_name} & {customer_name}"

        wcs_fields = {
            "call_title":            call_title,
            "call_date":             date_str,
            "call_time":             time_str + " UTC",
            "rep_name":              person_name,
            "call_type":             extracted.get("call_type") or extract_call_type_value(hs_summary),
            "deal_health":           extracted.get("deal_health") or grade,
            "deal_health_reason":    extracted.get("deal_health_reason"),
            "competitors_mentioned": extracted.get("competitors_mentioned") or nd,
            "deal_urgency":          extracted.get("deal_urgency") or extract_urgency(hs_summary),
            "what_brought_them":     extracted.get("what_brought_them"),
            "event_format_wcs":      extracted.get("event_format_wcs") or extract_event_format(hs_summary),
            "platform_wcs":          extracted.get("platform_wcs") or extract_platform(hs_summary),
            "languages_needed":      extracted.get("languages_needed") or details.get("languages") or nd,
            "audience_size":         extracted.get("audience_size") or details.get("audience_size"),
            "hours_needed":          extracted.get("hours_needed") or details.get("hours_needed"),
            "customer_type_wcs":     extracted.get("customer_type_wcs") or "direct",
            "internal_referral":     extracted.get("internal_referral", "false"),
            "pain_points":           extracted.get("pain_points"),
            "next_steps":            extracted.get("next_steps"),
            "meeting_summary":       hs_summary[:2000] if ok_hs else None,
            "audit_grade":           grade,
            "audit_details":         ((f"Transcript: {transcript_link}\n\n" if transcript_link else "") + mgmt_summary)[:5000] if ok_audit else None,
            "wordly_meeting_id":     m_hs_id or "",
            "wordly_transcript_id":  t_id,
        }
        print(f"    Writing custom object...", end=" ", flush=True)
        wcs_id = write_wcs_record(hs_key, wcs_fields, contact_id, details.get("company_id"))
        print(f"OK  WCS {wcs_id}" if wcs_id else "FAIL wcs")

        # Write note to secondary contacts on the meeting invite
        if m_hs_id:
            all_contact_ids = get_all_meeting_contacts(hs_key, m_hs_id)
            secondary = [cid for cid in all_contact_ids if cid != contact_id]
            if secondary:
                print(f"    Writing note to {len(secondary)} secondary contact(s)...")
                for sec_id in secondary:
                    write_hs_note(hs_key, sec_id, hs_note_body)

    # --- Competitive Intel ---
    if prompt_competitive:
        print(f"    Generating competitive intel...", end=" ", flush=True)
        comp   = gemini_call(f"{prompt_competitive}\n\n{call_context}", gemini_key)
        ok_c   = not comp.startswith(("ERROR", "EXCEPTION"))
        print("✅" if ok_c else "❌")
        if ok_c:
            save_file(GDRIVE_COMPETITIVE_FOLDER_ID, "Competitive", comp,
                      f"COMPETITIVE INTELLIGENCE\n"
                      f"Salesperson: {person_name} | Company: {company_name}\n"
                      f"Date: {m_start}")
        time.sleep(2)

    # --- Roadmap ---
    if prompt_roadmap:
        print(f"    Generating roadmap intel...", end=" ", flush=True)
        road   = gemini_call(f"{prompt_roadmap}\n\n{call_context}", gemini_key)
        ok_r   = not road.startswith(("ERROR", "EXCEPTION"))
        print("✅" if ok_r else "❌")
        if ok_r:
            save_file(GDRIVE_ROADMAP_FOLDER_ID, "RM", road,
                      f"ROADMAP & FEATURE REQUESTS\n"
                      f"Salesperson: {person_name} | Company: {company_name}\n"
                      f"Date: {m_start}")
        time.sleep(2)

    print(f"    Files saved: {file_base}-*.txt")

    # Slack fired at end of run, not per summary

    # Management review CSV
    review_csv_file_id = append_review_csv(drive_service, {
        "rep_name":        person_name,
        "meeting_date":    date_str,
        "meeting_time":    time_str,
        "company":         company_name,
        "customer_name":   customer_name,
        "hs_meeting_id":   m_hs_id or "",
        "transcript_id":   t_id,
        "match_confidence": conf,
        "grade":           grade_str,
        "deal_stage":      deal_stage or ""
    }, review_csv_file_id)

    # Mark processed — save immediately after each transcript
    processed[t_id] = {
        "processed_at": datetime.now(timezone.utc).isoformat(),
        "rep":          person_name,
        "company":      company_name,
        "date":         date_str,
        "grade":        grade_str
    }
    processed_file_id = save_processed(drive_service, processed, processed_file_id)

    return {"grade": grade, "note_id": note_id}, processed_file_id, review_csv_file_id


# ---------------------------------------------------------------------------
# PER-PERSON PIPELINE
# ---------------------------------------------------------------------------

def run_person(person, hs_key, gemini_key, slack_url, slack_intel,
               prompt_hs, prompt_mgmt, prompt_competitive, prompt_roadmap, prompt_intel,
               all_owners, processed, processed_file_id, review_csv_file_id,
               drive_service, lookback_hours=None, lookback_days=None):

    name  = person["name"]
    email = person["email"]
    section(f"{name} — {email}")

    wordly_key = get_wordly_key(email)
    if not wordly_key:
        print(f"  ❌  No Wordly key in Secret Manager")
        slack_notify(slack_url, f"⚠️ *Wordly key missing* for `{name}`")
        return {"name": name, "status": "no_key", "matched": 0, "summarized": 0}, \
               processed_file_id, review_csv_file_id

    owner    = resolve_owner(email, all_owners)
    owner_id = owner.get("id") if owner else None
    print(f"  {'✅  HubSpot owner ID: ' + owner_id if owner_id else '⚠️  Not found in HubSpot'}")

    meetings = []
    if owner_id:
        meetings = pull_hs_meetings(hs_key, owner_id,
                                    lookback_hours=lookback_hours,
                                    lookback_days=lookback_days)
        print(f"  HS meetings: {len(meetings)}")

    print(f"  Pulling Wordly transcripts...")
    transcripts = pull_wordly_transcripts(wordly_key,
                                          lookback_hours=lookback_hours,
                                          lookback_days=lookback_days)
    if transcripts is None:
        slack_notify(slack_url, f"⚠️ *Wordly auth failed* for `{name}`")
        return {"name": name, "status": "auth_failed", "matched": 0, "summarized": 0}, \
               processed_file_id, review_csv_file_id

    print(f"  Wordly transcripts: {len(transcripts)}")
    if not transcripts:
        return {"name": name, "status": "no_transcripts", "matched": 0, "summarized": 0}, \
               processed_file_id, review_csv_file_id

    if meetings:
        deduped = []
        used_times = []
        sorted_meetings = sorted(meetings,
            key=lambda m: (1 if m["title"].startswith("Calendly:") else 0,
                           m["start"] or datetime.min.replace(tzinfo=timezone.utc)))
        for m in sorted_meetings:
            if not m["start"]:
                deduped.append(m)
                continue
            is_dup = any(abs((m["start"] - t).total_seconds()) < 120 for t in used_times)
            if not is_dup:
                deduped.append(m)
                used_times.append(m["start"])
        if len(deduped) < len(meetings):
            print(f"  Deduped: {len(meetings)} -> {len(deduped)} ({len(meetings)-len(deduped)} Calendly duplicates removed)")
        meetings = deduped

    matches = match(transcripts, meetings) if meetings else [
        {"transcript": t, "meeting": None, "delta_mins": None,
         "score": 0, "confidence": "NONE"} for t in transcripts
    ]
    counts = Counter(r["confidence"] for r in matches)
    print(f"  Match: HIGH={counts['HIGH']} MEDIUM={counts['MEDIUM']} "
          f"LOW={counts['LOW']} NONE={counts['NONE']}")

    eligible   = [r for r in matches if r["confidence"] in ("HIGH", "MEDIUM")]

    # --- Session restart merge ---
    # If two eligible matches share the same company and are within 10 minutes
    # of each other, treat them as one session (restart detected).
    # Concatenate transcripts, process as one summary.
    from itertools import combinations
    MERGE_WINDOW_MINS = 10
    merged_ids = set()
    merged_matches = []

    for i, r1 in enumerate(eligible):
        if id(r1) in merged_ids:
            continue
        for r2 in eligible[i+1:]:
            if id(r2) in merged_ids:
                continue
            # Check same company and close start times
            c1 = r1.get("company_name", "") or ""
            c2 = r2.get("company_name", "") or ""
            t1 = r1["transcript"]["start"]
            t2 = r2["transcript"]["start"]
            if (c1 and c1 == c2 and t1 and t2):
                delta = abs((t1 - t2).total_seconds() / 60)
                if delta <= MERGE_WINDOW_MINS:
                    # Merge r2 into r1
                    r1["_merged"] = r2
                    merged_ids.add(id(r2))
                    break
        merged_matches.append(r1)

    eligible = merged_matches

    summarized = 0
    summaries_log = []
    for r in eligible:
        r["wordly_key"] = wordly_key
        result, processed_file_id, review_csv_file_id = summarize_match(
            r, name, hs_key, gemini_key,
            prompt_hs, prompt_mgmt, prompt_competitive, prompt_roadmap,
            processed, processed_file_id, review_csv_file_id,
            drive_service, slack_url
        )
        if result:
            summarized += 1
            m = r.get("meeting")
            summaries_log.append({
                "company":   r.get("company_name", "Unknown"),
                "call_type": r.get("call_type", "CALL"),
                "grade":     result.get("grade", "X"),
                "date_disp": r["transcript"]["start"].strftime("%b %d %I:%M %p") if r["transcript"]["start"] else ""
            })
        time.sleep(2)

    if person.get("intel_active") and prompt_intel:
        run_company_intel(name, meetings, hs_key, gemini_key, slack_intel,
                          prompt_intel, drive_service)

    return {"name": name, "status": "ok", "matched": len(matches),
            "summarized": summarized, "summaries_log": summaries_log,
            "missed": [
                {"meeting": r["meeting"]["title"][:50], "time": r["meeting"]["start"].strftime("%H:%M UTC") if r["meeting"]["start"] else "?", "reason": "NO_TRANSCRIPT"}
                for r in matches if r["confidence"] == "NONE" and r["meeting"]
            ] + [
                {"meeting": r["meeting"]["title"][:50], "time": r["meeting"]["start"].strftime("%H:%M UTC") if r["meeting"]["start"] else "?", "reason": "OUTSIDE_WINDOW"}
                for r in matches if r["confidence"] == "LOW" and r["meeting"]
            ]
        }, processed_file_id, review_csv_file_id


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    print()
    print("=" * 70)
    print("  WORDLY SALES INTELLIGENCE PIPELINE — v3.1 (GCP)")
    mode_str = f"BACKFILL ({BACKFILL_DAYS} days)" if IS_BACKFILL else f"HOURLY ({LOOKBACK_HOURS}h)"
    print(f"  Mode   : {mode_str}")
    print(f"  Filter : {TARGET_REP or 'ALL ACTIVE'}")
    print(f"  Run at : {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 70)

    print("\nLoading secrets...")
    hs_key      = get_secret(SECRET_HS_KEY)
    gemini_key  = get_secret(SECRET_GEMINI)
    slack_url   = get_secret(SECRET_SLACK)
    slack_intel = get_secret(SECRET_SLACK_INTEL)
    if not hs_key or not gemini_key:
        print("⛔  Missing HS or Gemini key.")
        sys.exit(1)

    print("Connecting to Google Drive...")
    drive_service = get_drive_service()

    print("Loading config from Drive...")
    salespeople = load_salespeople(drive_service)
    if not salespeople:
        print("⛔  No active salespeople.")
        sys.exit(1)

    prompt_hs          = load_prompt(drive_service, "prompt_hs.txt")
    prompt_mgmt        = load_prompt(drive_service, "prompt_sales_mgmt.txt")
    prompt_competitive = load_prompt(drive_service, "prompt_competitive.txt")
    prompt_roadmap     = load_prompt(drive_service, "prompt_roadmap.txt")
    prompt_intel       = load_prompt(drive_service, "prompt_company_intel.txt")

    if not prompt_hs or not prompt_mgmt:
        print("⛔  Missing core prompt files.")
        sys.exit(1)

    processed, processed_file_id = load_processed(drive_service)
    print(f"  Processed log: {len(processed)} entries")

    # Load review CSV file ID
    review_csv_file_id = drive_find_file(drive_service, REVIEW_CSV_FILENAME, GDRIVE_SRC_FOLDER_ID)

    salespeople_all = salespeople[:]
    salespeople = [p for p in salespeople if p.get("active")]
    if TARGET_REP:
        salespeople = [p for p in salespeople
                       if p["name"].lower() == TARGET_REP.lower()]

    print(f"\nFetching HubSpot owners...")
    all_owners = fetch_all_owners(hs_key)
    print(f"  {len(all_owners)} owners")

    lookback_hours = None
    lookback_days  = None
    if IS_BACKFILL:
        lookback_days = BACKFILL_DAYS
    else:
        lookback_hours = LOOKBACK_HOURS

    results = []
    for person in salespeople:
        result, processed_file_id, review_csv_file_id = run_person(
            person, hs_key, gemini_key, slack_url, slack_intel,
            prompt_hs, prompt_mgmt, prompt_competitive, prompt_roadmap, prompt_intel,
            all_owners, processed, processed_file_id, review_csv_file_id,
            drive_service,
            lookback_hours=lookback_hours, lookback_days=lookback_days
        )
        results.append(result)
        time.sleep(3)

    # ── Company Intel — runs independently for intel-flagged reps ──────────
    intel_reps = [p for p in salespeople_all if p.get("intel_active")]
    if intel_reps and prompt_intel:
        print(f"\nRunning company intel for {len(intel_reps)} rep(s)...")
        for person in intel_reps:
            owner    = resolve_owner(person["email"], all_owners)
            owner_id = owner.get("id") if owner else None
            if not owner_id:
                continue
            # Intel looks FORWARD 48 hours — upcoming meetings only
            from datetime import timedelta as td
            now_utc = datetime.now(timezone.utc)
            intel_since_ms = int((now_utc - td(hours=1)).timestamp() * 1000)
            intel_until_ms = int((now_utc + td(hours=48)).timestamp() * 1000)
            intel_meetings = []
            _res = requests.post(
                f"{HS_BASE_URL}/crm/v3/objects/meetings/search",
                headers={"Authorization": f"Bearer {hs_key}", "Content-Type": "application/json"},
                json={
                    "filterGroups": [{"filters": [
                        {"propertyName": "hubspot_owner_id", "operator": "EQ", "value": str(owner_id)},
                        {"propertyName": "hs_meeting_start_time", "operator": "GTE", "value": str(intel_since_ms)},
                        {"propertyName": "hs_meeting_start_time", "operator": "LTE", "value": str(intel_until_ms)}
                    ]}],
                    "properties": ["hs_meeting_title", "hs_meeting_start_time"],
                    "limit": 20
                }, timeout=15)
            if _res.status_code == 200:
                for _m in _res.json().get("results", []):
                    _p = _m.get("properties", {})
                    _t = _p.get("hs_meeting_start_time", "")
                    _title = _p.get("hs_meeting_title", "") or ""
                    if "cancel" in _title.lower() or "Calendly" in _title:
                        continue
                    intel_meetings.append({"hs_id": _m["id"], "title": _title,
                                           "start": _t, "start_str": _t})
            if intel_meetings:
                run_company_intel(person["name"], intel_meetings, hs_key,
                                  gemini_key, slack_intel, prompt_intel, drive_service)

    if slack_url and not IS_BACKFILL and datetime.now().weekday() == 1:
        try:
            all_hs_owners = fetch_all_owners(hs_key)
            csv_emails = {p["email"].lower() for p in salespeople_all}
            exclusions = {
                "kirk@wordly.ai", "vipul.vyas@wordly.ai", "chris.gillespie@wordly.ai",
                "marketing@wordly.ai", "newsletter@wordly.ai", "support@wordly.ai",
                "new-account@wordly.ai", "accounts@wordly.ai", "jill.czarnik@wordly.ai",
                "tyler.jeffrey@wordly.ai", "hayley.inkster@wordly.ai", "joao.garcia@wordly.ai",
                "chris.harget@wordly.ai", "lang@wordly.ai", "justin.cepelak@wordly.ai",
                "heather.hadley@wordly.ai", "chris.brophy@wordly.ai", "evie.parton@wordly.ai",
                "graham.herrli@wordly.ai", "katie.greathouse@wordly.ai", "diamone.floyd@wordly.ai",
                "jan.colson@wordly.ai", "nidhi.kapur@wordly.ai", "shereen.perillat@wordly.ai",
                "aki.hayashi@wordly.ai", "yorihito.hashimoto@wordly.ai", "lakshman@wordly.ai",
                "nicole.bertrand-haydari@wordly.ai", "amy.worboys@wordly.ai",
                "anastasia.gileva@wordly.ai", "shannon.long@wordly.ai", "dave.deasy@wordly.ai",
                "eric.paterson@wordly.ai", "scott.mceachen@wordly.ai", "molly.daft@wordly.ai",
                "wordly_apps@wordly.ai"
            }
            for owner in all_hs_owners:
                email = owner.get("email","").lower()
                if not email or not email.endswith("@wordly.ai") or email in exclusions:
                    continue
                if email not in csv_emails:
                    requests.post(slack_url, json={"text": f"⚠️ HubSpot owner not in salespeople.csv: `{owner.get('firstName','')} {owner.get('lastName','')}` ({email})"}, timeout=10)
        except Exception as e:
            print(f"  Missing rep check failed: {e}")

    section("PIPELINE COMPLETE")
    total = sum(r.get("summarized", 0) for r in results)

    # Daily missed summaries digest — fires once at 5 PM PST (01:00 UTC)
    now_utc = datetime.now(timezone.utc)
    if slack_url and not IS_BACKFILL and now_utc.hour == 1:
        try:
            # Use yesterday UTC date — calls processed during business hours land there
            yesterday_str = (now_utc - timedelta(hours=24)).strftime("%Y-%m-%d")
            today_str     = now_utc.strftime("%Y-%m-%d")
            day_entries   = {k: v for k, v in processed.items()
                             if v.get("date","") in (today_str, yesterday_str)}
            total_summarized = len(day_entries)
            by_rep = {}
            for v in day_entries.values():
                rep = v.get("rep","Unknown")
                by_rep[rep] = by_rep.get(rep, 0) + 1

            total_missed = sum(len(r.get("missed", [])) for r in results)

            lines = ["📊 *Daily Sales Pipeline Report*"]
            lines.append(f"Summarized today: {total_summarized} | Missed this run: {total_missed}")
            lines.append("")

            if by_rep:
                lines.append("*Summaries by rep:*")
                for rep, count in sorted(by_rep.items()):
                    lines.append(f"  • {rep}: {count}")
                lines.append("")

            has_misses = False
            for r in results:
                missed = r.get("missed", [])
                if missed:
                    has_misses = True
                    lines.append(f"*{r['name']}* — {len(missed)} missed:")
                    for m in missed:
                        lines.append(f"  • {m['time']} | {m['meeting']} | `{m['reason']}`")

            if not has_misses:
                lines.append("✅ No missed meetings in final run.")

            requests.post(slack_url, json={"text": "\n".join(lines)}, timeout=10)
        except Exception as e:
            print(f"  Daily digest failed: {e}")
    print(f"\n  {'Name':<30} {'Status':<15} {'Matched':>8} {'Summarized':>12}")
    print(f"  {'-'*30} {'-'*15} {'-'*8} {'-'*12}")
    for r in results:
        print(f"  {r['name']:<30} {r['status']:<15} "
              f"{r.get('matched',0):>8} {r.get('summarized',0):>12}")
    print(f"\n  Total summaries: {total}")

    # Slack: only fire end-of-run digest if there were errors
    # (per-summary notifications removed — errors handled inline)
    pass

    print()


if __name__ == "__main__":
    main()