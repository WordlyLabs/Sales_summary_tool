# Written by: Claude
# Origin: Claude chat, project "Transcripts to HubSpot" (exact chat title unknown)
# Date written: 2026-11-05
# Version: 1.0 - nightly copy of the HubSpot "Wordly Call Summary" records into BigQuery
#
# What it does: reads every Wordly Call Summary record from HubSpot, tidies each one into a row
# (one row per call), and replaces the table support-467322.sales_summaries.audit_scores.
# The table lives in us-central1. The location is fixed in this file on purpose.
# It reads the HubSpot key from Secret Manager (hs-service-key) and writes NOTHING to HubSpot.
# The table is replaced with a batch load, which BigQuery does not charge for.
# Runs as a Cloud Run job under the pipeline's service account.

import re
import sys
import time
from collections import Counter
from datetime import datetime, timezone

import requests
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, secretmanager

# ---------------------------------------------------------------------------
# SETTINGS
# ---------------------------------------------------------------------------
GCP_PROJECT = "support-467322"
BQ_DATASET = "sales_summaries"
BQ_TABLE = "audit_scores"
BQ_LOCATION = "us-central1"
HS_SECRET = "hs-service-key"
HS_BASE_URL = "https://api.hubapi.com"
HS_PORTAL_ID = "5315820"
WCS_OBJECT_TYPE_ID = "2-68535024"   # Wordly Call Summary custom object

AUDIT_LIMIT = 5000                  # the pipeline cuts audit text at this many characters
MAX_RETRIES = 5
MIN_KEEP_RATIO = 0.5                # refuse to replace the table if the new copy is under half the size
SEARCH_CAP_WARNING = 9500           # HubSpot search stops returning results at 10,000
EXCLUDE_TRANSCRIPT_IDS = {"test-record-do-not-use"}   # the test record created during setup

PROPERTIES = [
    "hs_createdate", "call_title", "call_date", "rep_name", "audit_grade", "deal_health",
    "audit_details", "wordly_transcript_id", "call_type", "competitors_mentioned",
    "deal_urgency", "languages_needed", "audience_size", "hours_needed",
    "event_format_wcs", "platform_wcs", "event_date_wcs", "pain_points", "next_steps",
    "meeting_summary", "package_hours", "quote_amount", "customer_type_wcs",
    "internal_referral", "what_brought_them", "deal_health_reason",
]

# HubSpot stores these dropdowns as codes. These are the labels people see in HubSpot.
LABELS = {
    "call_type": {"discovery_demo": "Discovery / Demo", "follow_up": "Follow-Up",
                  "proposal_pricing": "Proposal / Pricing", "closing": "Closing",
                  "exploratory": "Exploratory", "other": "Other"},
    "deal_urgency": {"tomorrow": "Tomorrow", "this_week": "This Week", "this_month": "This Month",
                     "this_quarter": "This Quarter", "this_year": "This Year",
                     "next_year": "Next Year", "other": "Other", "not_stated": "Not Stated"},
    "event_format_wcs": {"in_person": "In-person", "virtual": "Virtual", "hybrid": "Hybrid",
                         "not_stated": "Not Stated"},
    "platform_wcs": {"teams": "Teams", "zoom": "Zoom", "google_meet": "Google Meet",
                     "webex": "Webex", "goto": "GoTo", "in_person_only": "In-person only",
                     "other": "Other", "not_stated": "Not Stated"},
    "customer_type_wcs": {"direct": "Direct", "partner": "Partner", "reseller": "Reseller"},
}

SCHEMA = [
    bigquery.SchemaField("record_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("hubspot_record_link", "STRING"),
    bigquery.SchemaField("call_date", "DATE"),
    bigquery.SchemaField("rep_name", "STRING"),
    bigquery.SchemaField("customer", "STRING"),
    bigquery.SchemaField("company", "STRING"),
    bigquery.SchemaField("call_title", "STRING"),
    bigquery.SchemaField("audit_grade", "INT64"),
    bigquery.SchemaField("deal_health", "INT64"),
    bigquery.SchemaField("audit_text", "STRING"),
    bigquery.SchemaField("audit_text_cut", "BOOL"),
    bigquery.SchemaField("prompt_version", "STRING"),
    bigquery.SchemaField("transcript_link", "STRING"),
    bigquery.SchemaField("wordly_transcript_id", "STRING"),
    bigquery.SchemaField("call_type", "STRING"),
    bigquery.SchemaField("competitors_mentioned", "STRING"),
    bigquery.SchemaField("deal_urgency", "STRING"),
    bigquery.SchemaField("languages_needed", "STRING"),
    bigquery.SchemaField("audience_size", "FLOAT64"),
    bigquery.SchemaField("hours_needed", "FLOAT64"),
    bigquery.SchemaField("event_format", "STRING"),
    bigquery.SchemaField("platform", "STRING"),
    bigquery.SchemaField("event_date", "DATE"),
    bigquery.SchemaField("pain_points", "STRING"),
    bigquery.SchemaField("next_steps", "STRING"),
    bigquery.SchemaField("meeting_summary", "STRING"),
    bigquery.SchemaField("package_hours", "FLOAT64"),
    bigquery.SchemaField("quote_amount", "FLOAT64"),
    bigquery.SchemaField("customer_type", "STRING"),
    bigquery.SchemaField("internal_referral", "BOOL"),
    bigquery.SchemaField("what_brought_them", "STRING"),
    bigquery.SchemaField("deal_health_reason", "STRING"),
    bigquery.SchemaField("loaded_at", "TIMESTAMP"),
]

LINK_LINE = re.compile(r"^\s*Transcript:\s*(https://drive\.google\.com/file/d/\S+)\s*", re.IGNORECASE)


# ---------------------------------------------------------------------------
# SMALL CONVERTERS
# ---------------------------------------------------------------------------

def text_or_none(v):
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def to_int(v):
    try:
        return int(round(float(v)))
    except (TypeError, ValueError):
        return None


def to_float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def to_bool(v):
    s = str(v).strip().lower() if v is not None else ""
    if s == "true":
        return True
    if s == "false":
        return False
    return None


def to_date(v):
    """HubSpot dates arrive as 2026-10-05, as 2026-10-05T00:00:00Z, or as milliseconds."""
    if v is None:
        return None
    s = str(v).strip()
    if re.fullmatch(r"\d{12,13}", s):
        return datetime.fromtimestamp(int(s) / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    s = s[:10]
    return s if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s) else None


def label(field, value):
    v = text_or_none(value)
    if v is None:
        return None
    return LABELS.get(field, {}).get(v, v)


def parse_audit(raw):
    """Returns (audit text without the transcript line, transcript link, was it cut at the limit)."""
    raw = raw or ""
    cut = len(raw) >= AUDIT_LIMIT
    link = ""
    m = LINK_LINE.match(raw)
    if m:
        link = m.group(1)
        raw = raw[m.end():]
    return raw.strip(), link, cut


def prompt_version(text):
    """Which audit prompt wrote this audit, judged by its section headings."""
    if not text:
        return "none"
    up = text.upper()
    if "AREAS FOR DEVELOPMENT" in up:
        return "v2"
    if "AREAS TO LEAN INTO MORE" in up or "WHAT WORKED WELL" in up:
        return "v1"
    return "unknown"


# ---------------------------------------------------------------------------
# HUBSPOT
# ---------------------------------------------------------------------------

def get_secret(secret_id):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{GCP_PROJECT}/secrets/{secret_id}/versions/latest"
    return client.access_secret_version(request={"name": name}).payload.data.decode("utf-8").strip()


def hs_request(session, method, path, **kwargs):
    """One HubSpot call. If HubSpot says wait (429) or has a server error, wait and try again."""
    res = None
    for attempt in range(MAX_RETRIES):
        res = session.request(method, HS_BASE_URL + path, timeout=30, **kwargs)
        if res.status_code == 429 or res.status_code >= 500:
            if attempt == MAX_RETRIES - 1:
                break
            try:
                wait = float(res.headers.get("Retry-After", 2 ** attempt))
            except ValueError:
                wait = 2 ** attempt
            print(f"  HubSpot said wait (HTTP {res.status_code}); waiting {wait:.0f}s, "
                  f"try {attempt + 2} of {MAX_RETRIES}", flush=True)
            time.sleep(wait)
            continue
        res.raise_for_status()
        return res.json()
    res.raise_for_status()


def pull_records(session):
    records = []
    after = None
    while True:
        body = {"properties": PROPERTIES, "limit": 100,
                "sorts": [{"propertyName": "hs_createdate", "direction": "ASCENDING"}]}
        if after:
            body["after"] = after
        data = hs_request(session, "POST", f"/crm/v3/objects/{WCS_OBJECT_TYPE_ID}/search", json=body)
        records.extend(data.get("results", []))
        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            break
        time.sleep(0.3)   # the search call only allows a few requests a second
    if len(records) >= SEARCH_CAP_WARNING:
        print(f"  WARNING: {len(records)} records is close to HubSpot's 10,000 search limit. "
              "The loader needs to page by date before it gets there.")
    return records


def lookup_companies(session, record_ids):
    """record id -> company name. A failure here only leaves the company column blank."""
    names = {}
    try:
        company_of = {}
        for i in range(0, len(record_ids), 1000):
            chunk = record_ids[i:i + 1000]
            data = hs_request(session, "POST",
                              f"/crm/v4/associations/{WCS_OBJECT_TYPE_ID}/companies/batch/read",
                              json={"inputs": [{"id": rid} for rid in chunk]})
            for item in data.get("results", []):
                to = item.get("to") or []
                if to:
                    company_of[str(item["from"]["id"])] = str(to[0]["toObjectId"])
        unique = sorted(set(company_of.values()))
        name_of = {}
        for i in range(0, len(unique), 100):
            chunk = unique[i:i + 100]
            data = hs_request(session, "POST", "/crm/v3/objects/companies/batch/read",
                              json={"properties": ["name"], "inputs": [{"id": cid} for cid in chunk]})
            for item in data.get("results", []):
                name_of[str(item["id"])] = (item.get("properties") or {}).get("name") or ""
        for rid, cid in company_of.items():
            names[rid] = name_of.get(cid, "")
    except Exception as e:
        print(f"  WARNING: company lookup failed ({str(e)[:150]}). Company stays blank this run.")
    return names


# ---------------------------------------------------------------------------
# BUILDING THE ROWS
# ---------------------------------------------------------------------------

def _newest_key(rec):
    p = rec.get("properties") or {}
    rid = str(rec.get("id", ""))
    return (p.get("hs_createdate") or "", int(rid) if rid.isdigit() else 0)


def prepare_records(records):
    """Drop the setup test record. If one Wordly transcript has several records, keep the newest.
    Returns (records to load, test records dropped, older duplicates dropped)."""
    keep = []
    newest = {}
    with_id = 0
    excluded = 0
    for rec in records:
        tid = ((rec.get("properties") or {}).get("wordly_transcript_id") or "").strip()
        if tid in EXCLUDE_TRANSCRIPT_IDS:
            excluded += 1
            continue
        if not tid:
            keep.append(rec)
            continue
        with_id += 1
        cur = newest.get(tid)
        if cur is None or _newest_key(rec) > _newest_key(cur):
            newest[tid] = rec
    duplicates = with_id - len(newest)
    return keep + list(newest.values()), excluded, duplicates


def build_row(rec, companies, loaded_at):
    p = rec.get("properties") or {}
    rid = str(rec["id"])
    title = p.get("call_title") or ""
    customer = title.split("&", 1)[1].strip() if "&" in title else ""
    audit_text, link, cut = parse_audit(p.get("audit_details"))
    return {
        "record_id": rid,
        "hubspot_record_link": f"https://app.hubspot.com/contacts/{HS_PORTAL_ID}/record/{WCS_OBJECT_TYPE_ID}/{rid}",
        "call_date": to_date(p.get("call_date")),
        "rep_name": text_or_none(p.get("rep_name")),
        "customer": text_or_none(customer),
        "company": text_or_none(companies.get(rid)),
        "call_title": text_or_none(title),
        "audit_grade": to_int(p.get("audit_grade")),
        "deal_health": to_int(p.get("deal_health")),
        "audit_text": text_or_none(audit_text),
        "audit_text_cut": cut,
        "prompt_version": prompt_version(audit_text),
        "transcript_link": text_or_none(link),
        "wordly_transcript_id": text_or_none(p.get("wordly_transcript_id")),
        "call_type": label("call_type", p.get("call_type")),
        "competitors_mentioned": text_or_none(p.get("competitors_mentioned")),
        "deal_urgency": label("deal_urgency", p.get("deal_urgency")),
        "languages_needed": text_or_none(p.get("languages_needed")),
        "audience_size": to_float(p.get("audience_size")),
        "hours_needed": to_float(p.get("hours_needed")),
        "event_format": label("event_format_wcs", p.get("event_format_wcs")),
        "platform": label("platform_wcs", p.get("platform_wcs")),
        "event_date": to_date(p.get("event_date_wcs")),
        "pain_points": text_or_none(p.get("pain_points")),
        "next_steps": text_or_none(p.get("next_steps")),
        "meeting_summary": text_or_none(p.get("meeting_summary")),
        "package_hours": to_float(p.get("package_hours")),
        "quote_amount": to_float(p.get("quote_amount")),
        "customer_type": label("customer_type_wcs", p.get("customer_type_wcs")),
        "internal_referral": to_bool(p.get("internal_referral")),
        "what_brought_them": text_or_none(p.get("what_brought_them")),
        "deal_health_reason": text_or_none(p.get("deal_health_reason")),
        "loaded_at": loaded_at,
    }


# ---------------------------------------------------------------------------
# BIGQUERY
# ---------------------------------------------------------------------------

def load_table(rows):
    """Replace the table with a batch load (free). Returns (rows before, the table after)."""
    client = bigquery.Client(project=GCP_PROJECT, location=BQ_LOCATION)
    table_id = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    try:
        previous = client.get_table(table_id).num_rows
    except NotFound:
        previous = None
    if previous and len(rows) < previous * MIN_KEEP_RATIO:
        raise SystemExit(f"STOPPED: HubSpot gave {len(rows)} rows but the table already has {previous}. "
                         "The table was NOT replaced. Check HubSpot before running again.")
    job_config = bigquery.LoadJobConfig(
        schema=SCHEMA, write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
    job = client.load_table_from_json(rows, table_id, job_config=job_config)
    job.result()
    return previous, client.get_table(table_id)


def print_summary(rows, excluded, duplicates, previous, table):
    dates = sorted(r["call_date"] for r in rows if r["call_date"])
    versions = Counter(r["prompt_version"] for r in rows)
    print()
    print("=" * 60)
    print("  AUDIT LOADER DONE")
    print("=" * 60)
    print(f"  Rows loaded:                  {len(rows)}   (the table had {previous if previous is not None else 'nothing'} before)")
    print(f"  Setup test records dropped:   {excluded}")
    print(f"  Older duplicate records dropped (same Wordly transcript): {duplicates}")
    if dates:
        print(f"  Calls from {dates[0]} to {dates[-1]}")
    print(f"  With an audit grade:          {sum(1 for r in rows if r['audit_grade'] is not None)}")
    print(f"  Audit text by prompt version: " + ", ".join(f"{k} {v}" for k, v in sorted(versions.items())))
    print(f"  Audit text cut at {AUDIT_LIMIT} characters: {sum(1 for r in rows if r['audit_text_cut'])}")
    print(f"  With a transcript link:       {sum(1 for r in rows if r['transcript_link'])}")
    print(f"  With a company name:          {sum(1 for r in rows if r['company'])}")
    print(f"  Table: {table.full_table_id}   rows now: {table.num_rows}   location: {table.location}")
    print()


def main():
    print("=" * 60)
    print("  AUDIT LOADER v1.0 (HubSpot -> BigQuery, us-central1)")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60, flush=True)

    hs_key = get_secret(HS_SECRET)
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {hs_key}", "Content-Type": "application/json"})

    records = pull_records(session)
    print(f"  {len(records)} Wordly Call Summary records pulled from HubSpot", flush=True)
    if not records:
        raise SystemExit("STOPPED: HubSpot returned no records. The table was NOT touched.")

    records, excluded, duplicates = prepare_records(records)
    companies = lookup_companies(session, [str(r["id"]) for r in records])
    loaded_at = datetime.now(timezone.utc).isoformat()
    rows = [build_row(r, companies, loaded_at) for r in records]
    rows.sort(key=lambda r: (r["call_date"] or "", r["rep_name"] or "", r["record_id"]))

    previous, table = load_table(rows)
    print_summary(rows, excluded, duplicates, previous, table)


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as e:
        print(f"FAILED: {type(e).__name__}: {str(e)[:300]}", file=sys.stderr)
        raise
