import json
import os
import sys
import time
from urllib.parse import urlparse, parse_qs, unquote

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env")

if hasattr(sys.stdout, "reconfigure"):
    # Avoid Windows console crashes when a contact name includes characters
    # outside the active terminal code page.
    sys.stdout.reconfigure(errors="replace")

def load_hubspot_token():
    token = os.getenv("HUBSPOT_TOKEN", "").strip()

    if not token:
        raise ValueError("HUBSPOT_TOKEN was not found or is empty in the .env file")

    if any(char.isspace() for char in token):
        raise ValueError(
            "HUBSPOT_TOKEN contains whitespace. Copy the exact HubSpot token with no spaces or line breaks."
        )

    return token


HUBSPOT_TOKEN = load_hubspot_token()

CHECKPOINT_FILE = "checkpoint.json"   
CONTACTS_FILE = "contacts_report.xlsx"
OUTPUT_FILE = "documents_report.xlsx"
FAILED_FILE = "failed_contacts.xlsx"
REQUEST_TIMEOUT = 30
MAX_RETRIES = 4
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
SEARCH_PAGE_SIZE = 200
SEARCH_PAGE_DELAY_SECONDS = 0.25
CONTACT_PROCESS_LIMIT = None  # Set to None to process every matching contact in a full run.


def build_session():
    session = requests.Session()
    session.headers.update(
        {
            "Authorization": f"Bearer {HUBSPOT_TOKEN}",
            "Content-Type": "application/json",
        }
    )
    return session


SESSION = build_session()


def reset_session():
    global SESSION
    SESSION.close()
    SESSION = build_session()


def get_retry_delay(attempt, response=None):
    if response is not None:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                return max(float(retry_after), 0.0)
            except ValueError:
                pass

    return float(2 ** (attempt - 1))


def hubspot_request(method, url, **kwargs):
    last_exception = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = SESSION.request(method, url, timeout=REQUEST_TIMEOUT, **kwargs)

            if response.status_code in RETRYABLE_STATUS_CODES:
                if attempt == MAX_RETRIES:
                    print(response.text)
                    response.raise_for_status()

                delay = get_retry_delay(attempt, response=response)
                print(
                    f"{method} {url} returned {response.status_code}. Retrying in {delay:.1f}s "
                    f"({attempt}/{MAX_RETRIES})..."
                )
                reset_session()
                time.sleep(delay)
                continue

            return response
        except (requests.exceptions.ConnectionError, requests.exceptions.SSLError, requests.exceptions.Timeout) as exc:
            last_exception = exc

            if attempt == MAX_RETRIES:
                raise

            delay = get_retry_delay(attempt)
            print(
                f"{method} {url} failed with {type(exc).__name__}. Retrying in {delay:.1f}s "
                f"({attempt}/{MAX_RETRIES})..."
            )
            reset_session()
            time.sleep(delay)

    if last_exception:
        raise last_exception

    raise RuntimeError(f"{method} {url} failed without a response.")


def load_checkpoint():
    with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data["last_run"]


def search_modified_contacts(last_run, limit=SEARCH_PAGE_SIZE):
    search_url = "https://api.hubapi.com/crm/v3/objects/contacts/search"
    all_contacts = []
    after = None

    while True:
        payload = {
            "filterGroups": [
                {
                    "filters": [
                        {
                            "propertyName": "lastmodifieddate",
                            "operator": "GT",
                            "value": last_run,
                        }
                    ]
                }
            ],
            "properties": ["firstname", "lastname"],
            "limit": limit,
        }

        if after:
            payload["after"] = after

        response = hubspot_request("POST", search_url, json=payload)
        print("Search status code:", response.status_code)

        if response.status_code != 200:
            print(response.text)
            response.raise_for_status()

        data = response.json()
        results = data.get("results", [])

        if not results:
            break

        for contact in results:
            all_contacts.append(
                {
                    "contact_id": contact["id"],
                    "first_name": contact.get("properties", {}).get("firstname", ""),
                    "last_name": contact.get("properties", {}).get("lastname", ""),
                }
            )

        paging = data.get("paging", {})
        next_info = paging.get("next", {})
        after = next_info.get("after")

        if not after:
            break

        time.sleep(SEARCH_PAGE_DELAY_SECONDS)

    return all_contacts


def get_document_history(contact_id):
    url = (
        f"https://api.hubapi.com/crm/v3/objects/contacts/{contact_id}"
        "?properties=documentos&propertiesWithHistory=documentos"
    )
    response = hubspot_request("GET", url)
    response.raise_for_status()
    data = response.json()
    history_entries = data.get("propertiesWithHistory", {}).get("documentos", [])

    if history_entries:
        return history_entries

    current_value = data.get("properties", {}).get("documentos", "")
    if not current_value:
        return []

    return [
        {
            "timestamp": data.get("updatedAt", ""),
            "value": current_value,
        }
    ]


def extract_documents(contact):
    contact_id = contact["contact_id"]
    full_name = f"{contact['first_name']} {contact['last_name']}".strip()

    history_entries = get_document_history(contact_id)
    rows = []

    for entry in history_entries:
        timestamp = entry.get("timestamp", "")
        value = entry.get("value", "")

        if not value:
            continue

        urls = value.split(";")

        for file_url in urls:
            file_url = file_url.strip()
            if not file_url:
                continue

            parsed = urlparse(file_url)
            params = parse_qs(parsed.query)

            filename = params.get("filename", ["unknown_file"])[0]
            filename = unquote(filename)

            rows.append(
                {
                    "contact_id": contact_id,
                    "contact_name": full_name,
                    "document_timestamp": timestamp,
                    "document_filename": filename,
                    "document_url": file_url,
                }
            )

    return rows


def main():
    print("=== HUBSPOT DOCUMENT EXTRACTION ===")
    print()

    # print("Token loaded: YES")
    last_run = load_checkpoint()
    print("Checkpoint:", last_run)
    print()

    contacts = search_modified_contacts(last_run=last_run)
    # During testing, only process a small slice of the returned contacts.
    # Set CONTACT_PROCESS_LIMIT = None above to restore the full contact run.
    contacts_to_process = contacts[:CONTACT_PROCESS_LIMIT] if CONTACT_PROCESS_LIMIT is not None else contacts

    print()
    print("=== CONTACTS RETURNED IN THIS RUN ===")
    # Print only the contacts being processed in this test run.
    # The full result set is still exported to contacts_report.xlsx.
    for c in contacts_to_process:
        full_name = f"{c['first_name']} {c['last_name']}".strip()
        print(f"{c['contact_id']} - {full_name}")

    print()
    print("Total contacts found:", len(contacts))
    print("Contacts that will be processed in this run:", len(contacts_to_process))
    print()

    contacts_df = pd.DataFrame(contacts)
    if not contacts_df.empty:
        contacts_df.to_excel(CONTACTS_FILE, index=False)
        print(f"Saved: {CONTACTS_FILE}")

    all_documents = []
    failed_contacts = []

    for index, contact in enumerate(contacts_to_process, start=1):
        full_name = f"{contact['first_name']} {contact['last_name']}".strip()
        print(f"Checking contact {index}/{len(contacts_to_process)}: {contact['contact_id']} - {full_name}")

        try:
            docs = extract_documents(contact)
            all_documents.extend(docs)
        except Exception as e:
            failed_contacts.append(
                {
                    "contact_id": contact["contact_id"],
                    "contact_name": full_name,
                    "error": str(e),
                }
            )

        time.sleep(0.5)

    print()
    print("=== EXTRACTION SUMMARY ===")
    print("Total contacts checked:", len(contacts_to_process))
    print("Total documents extracted:", len(all_documents))
    print("Total failed contacts:", len(failed_contacts))
    print()

    print("=== DOCUMENT ARRANGEMENT PREVIEW ===")
    for item in all_documents[:6]:
        print("Contact ID:", item["contact_id"])
        print("Contact Name:", item["contact_name"])
        print("Document Timestamp:", item["document_timestamp"])
        print("Document Filename:", item["document_filename"])
        print("Document URL:", item["document_url"])
        print("-----")

    if all_documents:
        df = pd.DataFrame(all_documents)
        df.to_excel(OUTPUT_FILE, index=False)
        print(f"Saved: {OUTPUT_FILE}")

    if failed_contacts:
        failed_df = pd.DataFrame(failed_contacts)
        failed_df.to_excel(FAILED_FILE, index=False)
        print(f"Saved: {FAILED_FILE}")


if __name__ == "__main__":
    main()
