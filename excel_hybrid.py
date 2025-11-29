import os
import re
import threading
from time import sleep
from datetime import datetime, timedelta
import requests
import urllib.parse
from urllib.parse import urljoin
from urllib.parse import quote
import pandas as pd
from dotenv import load_dotenv
from openpyxl import load_workbook
import pycountry
from multiprocessing import Process, Queue
from openai import OpenAI
from find_job_titles import Finder
from langdetect import detect
import openai
import sys

# Load environment
load_dotenv(dotenv_path="./scraper.env")

# Google Sheet link
sheet_input = input("Please enter the google sheet link (Accounts tab): ").strip()

#GOOGLE_SHEET_URL = "https://docs.google.com/spreadsheets/d/1sto7OUVGs3E_GMv5jwhayDCLN9-_XkAsPGH-tOIfPBs/edit?usp=sharing"
GOOGLE_SHEET_URL = sheet_input

# Convert it to export format for pandas
GOOGLE_SHEET_BASE = GOOGLE_SHEET_URL.split("/edit")[0]
ACCOUNTS_URL = f"{GOOGLE_SHEET_BASE}/gviz/tq?tqx=out:csv&sheet=Accounts"
ISM_SHEET_NAME = "🎯 ISM"
ISM_URL = f"{GOOGLE_SHEET_BASE}/gviz/tq?tqx=out:csv&sheet={quote(ISM_SHEET_NAME)}"

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# --- Extract Google Sheet ID from the link ---
match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", GOOGLE_SHEET_URL)
if not match:
    raise ValueError("Invalid Google Sheet URL.")
SHEET_ID = match.group(1)

#OUTPUT_FILE = "LLM B&R stakeholders.xlsx"
OUTPUT_FILE ="testing_stakeholders.xlsx"

# Bypass API limits
class RateLimiter:
    def __init__(self, per_minute=200, per_hour=400):
        self.per_minute = per_minute
        self.per_hour = per_hour
        self.request_times = []
        self.lock = threading.Lock()

    def wait_for_slot(self):
        with self.lock:
            now = datetime.now()
            # Remove timestamps older than 60s and 3600s
            self.request_times = [t for t in self.request_times if now - t < timedelta(hours=1)]

            last_minute = [t for t in self.request_times if now - t < timedelta(minutes=1)]

            if len(last_minute) >= self.per_minute:
                sleep_time = 60 - (now - last_minute[0]).total_seconds()
                print(f"Minute cap hit. Resuming in {sleep_time:.1f}s...", flush=True)
                sleep(max(sleep_time, 0))

            if len(self.request_times) >= self.per_hour:
                sleep_time = 3600 - (now - self.request_times[0]).total_seconds()
                # local timestamp
                timestamp = now.strftime("%d/%m/%Y %-I:%M%p").lower().replace("am", "am").replace("pm", "pm")
                print(f"[{timestamp}] Hourly cap hit. Resuming in {sleep_time/60:.1f} min...", flush=True)
                sleep(max(sleep_time, 0))

            # Log request timestamp
            self.request_times.append(datetime.now())

def _process_wrapper(q, func, args, kwargs):
    """Runs a target function in a subprocess and pushes its result or exception to a queue."""
    try:
        q.put(func(*args, **kwargs))
    except Exception as e:
        q.put(e)

def run_with_timeout(func, timeout, *args, **kwargs):
    """Runs `func(*args, **kwargs)` with a timeout in seconds."""
    q = Queue()
    p = Process(target=_process_wrapper, args=(q, func, args, kwargs))
    p.start()
    p.join(timeout)

    if p.is_alive():
        print(f"Timeout after {timeout}s for {func.__name__}, killing process.", flush=True)
        p.terminate()
        p.join()
        return []

    result = q.get() if not q.empty() else []
    if isinstance(result, Exception):
        print(f"Exception in subprocess: {result}", flush=True)
        return []
    return result

def get_country_name(country_code):
    try:
        country = pycountry.countries.get(alpha_2=country_code)
        if country:
            return country.name
        else:
            return f"Country code {country_code} not found."
    except KeyError:
        return "Invalid country code."
    
finder = Finder()

def safe_findall(finder, text):
    try:
        return list(finder.findall(text))
    except StopIteration:
        return []   # no matches in this string
    
def get_roles_from_ism():
    try:
        df = pd.read_csv(ISM_URL)
        matched = set()
        split_pattern = re.compile(r"[•●▪–—/\n,]+")  # separators
        exclude_text = set([
        "true", "false", "audience:", "functions & hierarchy", "ideal stakeholder mapping",
        "only account level", "entry", "sign-off", "consensus", "roles",
        "primary decision makers", "key influencers (champions & gatekeepers)",
        "supporting and cross-functional stakeholders"
        ])

        for row in df.itertuples(index=False):
            for cell in row:
                if pd.isnull(cell):
                    continue
                text = str(cell).strip()
                if not text or text.lower() in exclude_text:
                    continue

                # Split multiple roles in the same cell
                parts = split_pattern.split(text)
                for part in parts:
                    part = part.strip()
                    if not part:
                        continue
                    try:
                        prompt = f"""
                        Extract all the text that resembles job titles or industries from the following text.
                        Return only a Python list of strings.
                        Text: \"\"\"{part}\"\"\"
                        """
                        response = client.chat.completions.create(
                            model="gpt-4.1-mini",
                            messages=[{"role": "user", "content": prompt}],
                            temperature=0
                        )
                        content = response.choices[0].message.content.strip()
                        roles_from_ai = eval(content) if content.startswith("[") else []
                        matched.update(roles_from_ai)
                    except Exception:
                        pass  # ignore errors

        roles = sorted(matched)
        return roles

    except Exception as e:
        print(f"Error loading roles from ISM sheet: {e}", flush=True)
        return []
    
roles = get_roles_from_ism()
print(roles)

import inflect
p = inflect.engine()


def generate_variations(roles_list):
    prompt = (
        "For each of the following job titles, generate 1-2 titles"
        "that are similar and are in the language of the given roles. Include seniority variants, synonyms, and expanded forms. "
        "Normalize ALL titles in roles_list."
        "Return a dictionary-like format"
        "with the original title as key and comma-separated variations as values.\n\n"
        f"Titles:\n{', '.join(roles_list)}"
    )
    try:
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a helpful assistant that generates job title variations."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
            max_tokens=500
        )
        out = response.choices[0].message.content.strip()

        role_variations = {}
        for line in out.split("\n"):
            if ":" in line:
                key, vals = line.split(":", 1)
                items = [v.strip() for v in re.split(r",|\n", vals) if v.strip()]
                role_variations[key.strip()] = items

        return role_variations

    except Exception as e:
        print(f"Error generating variations: {e}")
        return {}

# 5. Build the full expanded role list
role_variations_dict = generate_variations(roles)
# Start with the original roles
expanded_roles = roles.copy()

# Calculate how many extra slots we have
remaining_slots = 50 - len(expanded_roles)

# Flatten variations into a list while avoiding duplicates
variations_to_add = []
seen_lower = set([r.lower() for r in expanded_roles])

for orig, variations in role_variations_dict.items():
    for v in variations:
        clean = v.strip().strip('"').strip("'")
        if clean.lower() not in seen_lower:
            expanded_roles.append(clean)
            seen_lower.add(clean.lower())
            remaining_slots -= 1
        if remaining_slots <= 0:
            break
    if remaining_slots <= 0:
        break

# Add the variations
expanded_roles.extend(variations_to_add)
expanded_roles = [p.singular_noun(role) or role for role in expanded_roles]

print(f"Expanded roles list: {expanded_roles}")
roles = expanded_roles


seen_ids = set()
class ApolloClient:
    def __init__(self):
        self.api_key = os.getenv("APOLLO_API_KEY")
        self.base_url = "https://api.apollo.io/api/v1"
        self.headers = {
            "accept": "application/json",
            "Cache-Control": "no-cache",
            "Content-Type": "application/json",
            "X-Api-Key": self.api_key,
        }
        self.ratelimiter = RateLimiter()

    def search_company(self, company_name):
        #url = f"{self.base_url}/mixed_companies/search"
        url = f"{self.base_url}/organizations/search"
        payload = {
            "q_organization_name": company_name,
            "page": 1,
            "per_page": 100,
        }

        # Bypass API limits
        for attempt in range(5):
            self.ratelimiter.wait_for_slot()
            try:
                resp = requests.post(url, headers=self.headers, json=payload)
                if resp.status_code == 429:
                    print(f"[Apollo] Rate limit hit, retrying in 10s (attempt {attempt+1}/5)...", flush=True)
                    sleep(10)
                    continue
                if 500 <= resp.status_code < 600:
                    print(f"[Apollo] Server error {resp.status_code}, retrying in 5s...", flush=True)
                    sleep(5)
                    continue
                break
            except requests.exceptions.RequestException as e:
                print(f"[Apollo] Request error: {e}, retrying in 5s...", flush=True)
                sleep(5)
        else:
            print("[Apollo] Too many retries. Skipping.")
            return None

        #resp = requests.post(url, headers=self.headers, json=payload)
        if resp.status_code != 200:
            return None
        data = resp.json()
        companies = data.get("accounts") or data.get("organizations") or []
        if not companies:
            return None
        company = companies[0]
        return {
            "org_id": company.get("organization_id"),
            "q_organization_name": company.get("company_name"),
            #"country": company.get("organization_location"),
            "domain": company.get("website_url") or company.get("domain"),
        }
    
    def _country_names_from_alpha2_list(self, alpha2_list):
        """
        Given a list of alpha2 codes, return the corresponding country names
        for the Apollo payload. Ignore invalid alpha2 values.
        """
        names = []
        for c in alpha2_list:
            if not c:
                continue
            try:
                country_obj = pycountry.countries.get(alpha_2=c.upper())
                if country_obj:
                    names.append(country_obj.name)
            except Exception:
                continue
        return names
    
    def resolve_country_codes(self, hq_value, target_value):

        # --- 1. Convert HQ alpha2 → full country name ---
        hq_country = None
        if isinstance(hq_value, str) and hq_value.strip():
            hq_value_clean = hq_value.strip()

            # If more than 2 characters → treat as full country name
            if len(hq_value_clean) > 2:
                hq_country = hq_value_clean

            else:
                # Treat as alpha2
                hq_alpha2 = hq_value_clean.upper()
                obj = pycountry.countries.get(alpha_2=hq_alpha2)
                hq_country = obj.name if obj else None

        # --- 2. Parse Target countries (KEEP AS NORMAL NAMES) ---
        targets = []
        if isinstance(target_value, str) and target_value.strip():

            # replace connectors with comma
            cleaned = re.sub(r"\s+and\s+|&", ",", target_value, flags=re.IGNORECASE)
            parts = [p.strip() for p in cleaned.split(",") if p.strip()]

            # keep them exactly as they are
            targets = parts

        # --- 3. Remove HQ if repeated ---
        if hq_country:
            targets = [t for t in targets if t.lower() != hq_country.lower()]

        # --- 4. Return list in NORMAL COUNTRY FORM ---
        final = []
        if hq_country:
            final.append(hq_country)

        final.extend(targets)

        return final

    def search_contacts(self, org_id=None, domain=None, country=None, page=1, roles=None, per_page=100):
        all_results = []
        form_headers = self.headers.copy()
        form_headers["Content-Type"] = "application/x-www-form-urlencoded"

        # Define payload before using it
        payload = {
            "page": page,
            "per_page": per_page,
            "include_similar_titles": True,
        }

        if org_id:
            payload["organization_ids[]"] = [org_id]
        
        if domain:
            payload["q_organization_domains_list[]"] = [domain]
        else:
            payload["q_organization_domains_list[]"] = None

        # if country:
        #     country_obj = pycountry.countries.get(alpha_2=country.upper())
        #     if country_obj:
        #         payload["organization_location"] = country_obj.name
        #         payload["person_locations[]"] = [country_obj.name]
        #         #print(f"[DEBUG] Added location filter: {country_obj.name}")

        if country:
            # ensure it's a list
            if isinstance(country, str):
                country_list = [country]
            else:
                country_list = list(country)

            # clean up spacing
            country_list = [c.strip() for c in country_list if c.strip()]

            if country_list:
                payload["person_locations[]"] = country_list
                payload["organization_location"] = country_list[0]
                print(f"[DEBUG] Added location filters: {country_list}", flush=True)
            
            else:
                payload["person_locations[]"] = None
            
        else:
             payload["person_locations[]"] = None

        if roles:
            if isinstance(roles, str):
                roles = [roles]
            for role in roles:
                payload.setdefault("person_titles[]", []).append(role.strip())
            
        # --- 1. Contacts search first ---
        url_contacts = f"{self.base_url}/contacts/search"
        contacts = []
        try:
            #print(f"[DEBUG] Calling Apollo Contacts API with role filters: {payload.get('person_titles[]')}", flush=True)
            resp_c = requests.post(url_contacts, headers=form_headers, data=urllib.parse.urlencode(payload, doseq=True))
            if resp_c.status_code == 200:
                data_c = resp_c.json()
                contacts = data_c.get("contacts", [])
                for c in contacts:
                    cid = c.get("id")
                    if cid and cid not in seen_ids:
                        seen_ids.add(cid)
                        all_results.append(c)
            else:
                print(f"[ERROR] Contacts search failed ({resp_c.status_code}): {resp_c.text}", flush=True)
        except Exception as e:
            print(f"[ERROR] Contacts search request failed: {e}", flush=True)

        pagination_info = {}
        if "data_c" in locals() and isinstance(data_c.get("pagination"), dict):
            pagination_info["contacts"] = data_c.get("pagination")
        #print(f"[DEBUG] Payload being sent: {payload}")
        return all_results, pagination_info
        #return all_results, data.get("pagination", {}) if 'data' in locals() else {}
    
    def search_people(self, org_id=None, domain=None, country=None, page=1, roles=None, per_page=100):
        all_results = []
        form_headers = self.headers.copy()
        form_headers["Content-Type"] = "application/x-www-form-urlencoded"
        # --- 2. People search ---
        payload = {
            "page": page,
            "per_page": per_page,
            "include_similar_titles": True,
        }

        if org_id:
            payload["organization_ids[]"] = [org_id]
        
        if domain:
            payload["q_organization_domains_list[]"] = [domain]
        else:
            payload["q_organization_domains_list[]"] = None

        # if country:
        #     country_obj = pycountry.countries.get(alpha_2=country.upper())
        #     if country_obj:
        #         payload["organization_location"] = country_obj.name
        #         payload["person_locations[]"] = [country_obj.name]
        #         #print(f"[DEBUG] Added location filter: {country_obj.name}")

        if country:
            # ensure it's a list
            if isinstance(country, str):
                country_list = [country]
            else:
                country_list = list(country)

            # clean up spacing
            country_list = [c.strip() for c in country_list if c.strip()]

            if country_list:
                payload["person_locations[]"] = country_list
                payload["organization_location"] = country_list[0]
                print(f"[DEBUG] Added location filters: {country_list}", flush=True)
            
            else:
                payload["person_locations[]"] = None
        
        else:
             payload["person_locations[]"] = None
                
        if roles:
            if isinstance(roles, str):
                roles = [roles]
            for role in roles:
                payload.setdefault("person_titles[]", []).append(role.strip())

        #payload["person_seniorities[]"] = ["owner", "founder", "c_suite", "partner", "vp", "head", "director", "manager"]

        url = f"{self.base_url}/people/search"
        people = []
        try:
            #print(f"[DEBUG] Calling Apollo Contacts API with role filters: {payload.get('person_titles[]')}", flush=True)
            resp = requests.post(url, headers=form_headers, data=urllib.parse.urlencode(payload, doseq=True))
            if resp.status_code == 200:
                data_p = resp.json()
                people = data_p.get("people", [])
                for p in people:
                    pid = p.get("id")
                    if pid and pid not in seen_ids:
                        seen_ids.add(pid)
                        all_results.append(p)
                #print(f"[DEBUG] Found {len(people)} people")
            else:
                print(f"[ERROR] People search failed ({resp.status_code}): {resp.text}", flush=True)
                people = []
        except Exception as e:
            print(f"[ERROR] People search request failed: {e}", flush=True)
            people = []

        #all_results.extend(people)
        pagination_info = {}
        # if "data_c" in locals() and isinstance(data_c.get("pagination"), dict):
        #     pagination_info["contacts"] = data_c.get("pagination")
        if "data_p" in locals() and isinstance(data_p.get("pagination"), dict):
            pagination_info["people"] = data_p.get("pagination")
        #print(f"[DEBUG] Payload being sent: {payload}")
        return all_results, pagination_info


def main():
    accounts_df = pd.read_csv(ACCOUNTS_URL)
    accounts_df.columns = accounts_df.columns.str.strip()
    accounts_df = accounts_df.drop_duplicates(subset="Account Name")

    account_names = accounts_df["Account Name"].dropna().tolist()
    if "Company Website" in accounts_df.columns:
        account_domains = accounts_df["Company Website"].dropna().tolist()
    elif "Website" in accounts_df.columns:
        account_domains = accounts_df["Website"].dropna().tolist()
    else:
        account_domains = []
    alpha2_codes = accounts_df["HQ Country"].tolist()

    # FOR TESTING (REMOVE LATER): Limit to the first few rows
    accounts_df = accounts_df.head(5)
    account_names = accounts_df["Account Name"].dropna().tolist()
    if "Company Website" in accounts_df.columns:
        account_domains = accounts_df["Company Website"].dropna().tolist()
    elif "Website" in accounts_df.columns:
        account_domains = accounts_df["Website"].dropna().tolist()
    else:
        account_domains = []
    alpha2_codes = accounts_df["HQ Country"].tolist()
    #REMOVE ^^^^^^^^

    apollo = ApolloClient()
    all_people = []
    missing_accounts = []

    def process_company(acc, domain, alpha2, max_entries_per_company):
        combined_results = []
        target_val = None
        if "Target countries" in accounts_df.columns:
            row = accounts_df.loc[accounts_df["Account Name"] == acc]
            if len(row):
                target_val = row["Target countries"].iloc[0]

        country_code = apollo.resolve_country_codes(alpha2, target_val)

        found_any = False
        org_info = apollo.search_company(acc)
        source = "Apollo"
        #domain = None
        domain = domain or org_info.get("domain")

        if org_info:
            page = 1
            seen_person_ids = set()
            seen_roles = set()  # track which roles already added
            seen_titles = set()
            #found_any = False
            
            while True:
                apollo.ratelimiter.wait_for_slot()
                print(f"Apollo domain: {org_info.get('domain')}", flush=True)
                contacts, pagination_c = apollo.search_contacts(
                    org_id=org_info["org_id"],
                    #domain = org_info["domain"],
                    domain = domain,
                    country=country_code,
                    roles=roles,
                    #roles=None,
                    page=page,
                    per_page=100
                )

                for contact in contacts:
                    person_id = contact.get("id") or contact.get("contact_id")
                    if person_id and person_id in seen_person_ids:
                        continue

                    title = contact.get("title")
                    if not title:
                        continue

                    title_norm = re.sub(r"[^\w\s]", " ", title.lower())
                    title_norm = re.sub(r"\s+", " ", title_norm).strip()

                    if title_norm in seen_titles:
                        continue

                    if any(re.search(rf"\b{re.escape(r.lower())}\b", title_norm) for r in seen_roles):
                        continue

                    formatted_contact = {
                        "Accounts": acc,
                        "Source": source,
                        "First name (Required - FREE TEXT)": contact.get("first_name", ""),
                        "Last name (Required - FREE TEXT)": contact.get("last_name", ""),
                        "Position (Required - FREE TEXT)": contact.get("title", ""),
                        "Stakeholder Location (ALPHA2 Country Code)": contact.get("country", country_code),
                        "Email": contact.get("email", ""),
                        "Personal Linkedin URL": contact.get("linkedin_url", "")
                    }
                    combined_results.append(formatted_contact)
                    if person_id:
                        seen_person_ids.add(person_id)

                    found_any = True
                    seen_titles.add(title_norm)

                    for r in roles:
                        r_norm = r.lower().strip()
                        if re.search(rf"\b{re.escape(r_norm)}\b", title_norm):
                            seen_roles.add(r_norm)
                            break

                apollo.ratelimiter.wait_for_slot()
                people, pagination_p = apollo.search_people(
                    org_id=org_info["org_id"],
                    domain = org_info["domain"],
                    country=country_code,
                    roles=roles,
                    #roles=None,
                    page=page,
                    per_page=100,
                )

                for person in people:
                    person_id = person.get("id") or person.get("person_id")
                    if person_id and person_id in seen_person_ids:
                        continue
                    title = person.get("title", "")
                    if not title:   # skip if title is missing or empty
                        continue

                    title_norm = re.sub(r"[^\w\s]", " ", title.lower())
                    title_norm = re.sub(r"\s+", " ", title_norm).strip()

                    if title_norm in seen_titles:
                        continue

                    if any(re.search(rf"\b{re.escape(r.lower())}\b", title_norm) for r in seen_roles):
                        continue

                    formatted_person = {
                        "Accounts": acc,
                        "Source": source,
                        "First name (Required - FREE TEXT)": person.get("first_name", ""),
                        "Last name (Required - FREE TEXT)": person.get("last_name", ""),
                        "Position (Required - FREE TEXT)": person.get("title", ""),
                        "Stakeholder Location (ALPHA2 Country Code)": person.get("country", country_code),
                        "Email": person.get("email", ""),
                        "Personal Linkedin URL": person.get("linkedin_url", "")
                    }
                    combined_results.append(formatted_person)
                    if person_id:
                        seen_person_ids.add(person_id)

                    found_any = True
                    seen_titles.add(title_norm)

                    for r in roles:
                        r_norm = r.lower().strip()
                        if re.search(rf"\b{re.escape(r_norm)}\b", title_norm):
                            seen_roles.add(r_norm)
                            break

                print(f"Number of people found:{len(combined_results)}", flush=True)
                if len(combined_results) >= max_entries_per_company:
                    print(f"Reached max entries ({max_entries_per_company}) for {acc}", flush=True)
                    return combined_results[:max_entries_per_company]

                #total_pages = pagination.get("total_pages", page)
                total_pages = max(
                pagination_c.get("total_pages", 1),
                pagination_p.get("total_pages", 1),
                )   
                if page >= total_pages or len(seen_roles) == max_entries_per_company:
                    break
                page += 1
        
        else:
            missing_accounts.append({"Account Name": acc, "HQ Country": country_code})
            #print(f"Combined results length:{len(results)}")
            return combined_results

        if not found_any:
            print(f"No Apollo match for {acc}", flush=True)
            missing_accounts.append({"Account Name": acc, "HQ Country": country_code})
        sleep(2)
        return combined_results

    # while True:
    #     user_input = input("Enter the maximum number of stakeholder entries per company: ").strip()
    #     try:
    #         max_entries_per_company = int(user_input)
    #         print(f"\nLimiting to {max_entries_per_company} entries per company.\n", flush=True)
    #         break  # exit loop when valid input is given
    #     except ValueError:
    #         print("Invalid input. Please enter a valid number.\n")

    if len(sys.argv) < 3:
        print("Error: Missing required arguments (sheet_url, max_entries_per_company).")
        return

    sheet_url = sys.argv[1]
    max_entries_per_company = int(sys.argv[2])

    print(f"Using sheet: {sheet_url}", flush=True)
    print(f"Limiting to {max_entries_per_company} entries per company.\n", flush=True)

    accounts_df = pd.read_csv(sheet_url)

    for acc, domain, alpha2 in zip(account_names, account_domains, alpha2_codes):
        try:
            people = process_company(acc, domain, alpha2, max_entries_per_company)
            if people:
                for p in people:
                    all_people.append(p)
        except Exception as e:
            print(f"[ERROR] Failed to process {acc}: {e}", flush=True)
        sleep(2)  # small delay between accounts to stay under rate limits


    # Save results
    if all_people or missing_accounts:
        #new_df = pd.DataFrame(all_people)
        # Just testing out before saving
        #print(new_df)
        with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
            if all_people:
                pd.DataFrame(all_people).to_excel(writer, sheet_name="stakeholders", index=False)
                print(f"Saved {len(all_people)} contacts to 'stakeholders' sheet.", flush=True)
            if missing_accounts:
                pd.DataFrame(missing_accounts).to_excel(writer, sheet_name="missing", index=False)
                print(f"Saved {len(missing_accounts)} missing accounts to 'missing' sheet.", flush=True)
    else:
        print("No contacts found.")

if __name__ == "__main__":
    main()

