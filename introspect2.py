"""Focused follow-up introspection: active-campaign steps, nested warmup data, lead status variety."""
import os, json, warnings
from datetime import datetime, timedelta
warnings.filterwarnings("ignore")
import requests
from dotenv import load_dotenv
load_dotenv()

BASE = "https://api.instantly.ai/api/v2"
KEY = os.getenv("INSTANTLY_API_KEY", "").strip()
H = {"Authorization": f"Bearer {KEY}", "Content-Type": "application/json"}

def get(path, **params):
    return requests.get(f"{BASE}{path}", headers=H, params=params, timeout=30)
def post(path, body):
    return requests.post(f"{BASE}{path}", headers=H, json=body, timeout=30)

def line(t): print(f"\n=== {t} ===")

# 1. Find an ACTIVE campaign (status 1) and pull its steps
line("STEP ANALYTICS on an active campaign")
camps = get("/campaigns", limit=100).json()
items = camps.get("items", camps if isinstance(camps, list) else [])
active = [c for c in items if c.get("status") == 1] or items
print(f"campaigns: {len(items)} total, {len([c for c in items if c.get('status')==1])} active (status=1)")
for c in active[:6]:
    cid = c.get("id")
    steps = get("/campaigns/analytics/steps", id=cid).json()
    slist = steps if isinstance(steps, list) else steps.get("items", [])
    total_sent = sum(s.get("sent", 0) for s in slist)
    if total_sent > 0:
        print(f"\ncampaign {cid} (status {c.get('status')}) — steps with data:")
        print(json.dumps(slist, indent=2)[:1500])
        break
else:
    print("no campaign with step sends found in first 6 active")

# 2. Drill into warmup analytics nested structure
line("WARMUP ANALYTICS nested fields (deliverability)")
accts = get("/accounts", limit=10).json()
alist = accts.get("items", accts if isinstance(accts, list) else [])
emails = [a.get("email") for a in alist if a.get("email")][:3]
print(f"probing warmup for {len(emails)} mailbox(es)")
wa = post("/accounts/warmup-analytics", {"emails": emails}).json()
agg = wa.get("aggregate_data", {})
edd = wa.get("email_date_data", {})
if agg:
    first_mbox = list(agg.keys())[0]
    print(f"\naggregate_data['<mailbox>'] fields:")
    print(json.dumps(agg[first_mbox], indent=2)[:800])
if edd:
    first_mbox = list(edd.keys())[0]
    sample = edd[first_mbox]
    print(f"\nemail_date_data['<mailbox>'] shape: {type(sample).__name__}")
    if isinstance(sample, dict):
        k = list(sample.keys())[0]
        print(f"  keyed by date, e.g. '{k}' = {json.dumps(sample[k])[:300]}")
    elif isinstance(sample, list) and sample:
        print(f"  list item: {json.dumps(sample[0])[:300]}")

# 3. Lead status variety + interest/ai fields
line("LEAD status codes + interest fields")
leads = post("/leads/list", {"limit": 50}).json()
llist = leads.get("items", leads if isinstance(leads, list) else [])
statuses = {}
interest_fields = set()
for l in llist:
    s = l.get("status")
    statuses[s] = statuses.get(s, 0) + 1
    for k in l.keys():
        if "interest" in k.lower() or "ai_" in k.lower() or k == "lt_interest_status":
            interest_fields.add(k)
print(f"distinct lead status values seen: {dict(sorted(statuses.items(), key=lambda x: str(x[0])))}")
print(f"interest/ai fields present on leads: {interest_fields or 'none in this sample'}")
# show a lead that has engagement
engaged = [l for l in llist if l.get("email_open_count", 0) > 1 or l.get("email_reply_count", 0) > 0]
if engaged:
    l = engaged[0]
    print(f"\nsample engaged lead (masked): opens={l.get('email_open_count')}, "
          f"replies={l.get('email_reply_count')}, status={l.get('status')}, "
          f"status_summary={l.get('status_summary')}")

# 4. Campaign status code meanings (infer from data)
line("CAMPAIGN status codes seen")
cstat = {}
for c in items:
    s = c.get("status")
    cstat[s] = cstat.get(s, 0) + 1
print(f"distinct campaign status values: {dict(sorted(cstat.items(), key=lambda x: str(x[0])))}")
print("(Instantly: 0=draft, 1=active, 2=paused, 3=completed, 4=running subsequences, -99=suspended)")
