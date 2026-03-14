import re
from collections import defaultdict
from urllib.parse import urlparse

import pandas as pd
import requests

from config import HUBSPOT_TOKEN

DOCUMENT_REPORT_FILE = "document_report.xlsx"
OUTPUT_FILE = "documents_to_process.xlsx"
REQUEST_TIMEOUT_SECONDS = 30
ASSOCIATION_BATCH_SIZE = 1000
NOTE_BATCH_SIZE = 100
EXPECTED_NO_ASSOCIATION_SUBCATEGORY = "crm.associations.NO_ASSOCIATIONS_FOUND"

HEADERS = {
    "Authorization": f"Bearer {HUBSPOT_TOKEN}",
    "Content-Type": "application/json",
}


def chunked(values, size):
    for index in range(0, len(values), size):
        yield values[index:index + size]


def extract_file_id_from_url(file_url):
    if not isinstance(file_url, str) or not file_url.strip():
        return None

    parsed_url = urlparse(file_url)
    path = parsed_url.path or ""

    patterns = [
        r"/signed-url-redirect/(\d+)(?:$|/)",
        r"/files/v3/files/(\d+)(?:$|/)",
        r"/filemanager/api/v3/files/(\d+)(?:$|/)",
    ]

    for pattern in patterns:
        match = re.search(pattern, path)
        if match:
            return match.group(1)

    query_match = re.search(r"(?:^|[?&])fileId=(\d+)(?:$|&)", parsed_url.query or "")
    if query_match:
        return query_match.group(1)

    return None


def normalize_filename(filename):
    if not isinstance(filename, str):
        return None

    normalized = filename.strip()
    if not normalized:
        return None

    return normalized.casefold()


def hubspot_request(method, url, **kwargs):
    return requests.request(
        method,
        url,
        headers=HEADERS,
        timeout=REQUEST_TIMEOUT_SECONDS,
        **kwargs,
    )


def parse_json_response(response, allowed_status_codes, error_label):
    if response.status_code not in allowed_status_codes:
        print(f"{error_label} failed with status code: {response.status_code}")
        print(response.text)
        return None

    return response.json()


def log_unexpected_batch_errors(errors, label):
    unexpected_errors = [
        error
        for error in errors
        if error.get("subCategory") != EXPECTED_NO_ASSOCIATION_SUBCATEGORY
    ]

    if not unexpected_errors:
        return

    print(f"{label} returned {len(unexpected_errors)} unexpected batch errors.")
    for error in unexpected_errors[:3]:
        print(error)


def fetch_note_ids_by_contact(contact_ids):
    note_ids_by_contact = {contact_id: set() for contact_id in contact_ids}
    pending_inputs = [{"id": contact_id} for contact_id in contact_ids]

    while pending_inputs:
        next_inputs = []

        for batch in chunked(pending_inputs, ASSOCIATION_BATCH_SIZE):
            response = hubspot_request(
                "POST",
                "https://api.hubapi.com/crm/v4/associations/contacts/notes/batch/read",
                json={"inputs": batch},
            )

            data = parse_json_response(
                response,
                allowed_status_codes={200, 207},
                error_label="Note association scan",
            )
            if data is None:
                continue
            log_unexpected_batch_errors(data.get("errors", []), "Note association scan")

            for result in data.get("results", []):
                from_info = result.get("from", {})
                contact_id = str(from_info.get("id", "")).strip()
                if not contact_id:
                    continue

                for association in result.get("to", []):
                    note_id = str(association.get("toObjectId", "")).strip()
                    if note_id:
                        note_ids_by_contact.setdefault(contact_id, set()).add(note_id)

                next_after = result.get("paging", {}).get("next", {}).get("after")
                if next_after:
                    next_inputs.append({"id": contact_id, "after": next_after})

        pending_inputs = next_inputs

    return note_ids_by_contact


def fetch_attachment_ids_by_note(note_ids):
    attachment_ids_by_note = {}

    for batch in chunked(note_ids, NOTE_BATCH_SIZE):
        response = hubspot_request(
            "POST",
            "https://api.hubapi.com/crm/v3/objects/notes/batch/read",
            json={
                "properties": ["hs_attachment_ids"],
                "inputs": [{"id": note_id} for note_id in batch],
            },
        )

        data = parse_json_response(
            response,
            allowed_status_codes={200, 207},
            error_label="Note property scan",
        )
        if data is None:
            continue
        log_unexpected_batch_errors(data.get("errors", []), "Note property scan")

        for note in data.get("results", []):
            note_id = str(note.get("id", "")).strip()
            attachment_value = note.get("properties", {}).get("hs_attachment_ids", "") or ""
            attachment_ids = {
                part.strip()
                for part in re.split(r"[;,]", attachment_value)
                if part.strip()
            }
            attachment_ids_by_note[note_id] = attachment_ids

    return attachment_ids_by_note


def fetch_attachment_ids_by_contact(contact_ids):
    note_ids_by_contact = fetch_note_ids_by_contact(contact_ids)
    all_note_ids = sorted(
        {
            note_id
            for note_ids in note_ids_by_contact.values()
            for note_id in note_ids
        }
    )

    attachment_ids_by_contact = defaultdict(set)
    for contact_id in contact_ids:
        attachment_ids_by_contact[contact_id] = set()

    if not all_note_ids:
        return attachment_ids_by_contact, note_ids_by_contact

    attachment_ids_by_note = fetch_attachment_ids_by_note(all_note_ids)

    for contact_id, note_ids in note_ids_by_contact.items():
        for note_id in note_ids:
            attachment_ids_by_contact[contact_id].update(
                attachment_ids_by_note.get(note_id, set())
            )

    return attachment_ids_by_contact, note_ids_by_contact


def fetch_attachment_names_by_id(attachment_ids):
    attachment_names_by_id = {}
    missing_file_scope = False

    for attachment_id in sorted(attachment_ids):
        response = hubspot_request(
            "GET",
            f"https://api.hubapi.com/files/v3/files/{attachment_id}",
        )

        if response.status_code == 200:
            data = response.json()
            filename = normalize_filename(data.get("name"))
            if filename:
                attachment_names_by_id[attachment_id] = filename
            continue

        if response.status_code == 403 and "MISSING_SCOPES" in response.text:
            missing_file_scope = True
            break

        if response.status_code == 404:
            continue

        print(f"Attachment filename lookup failed for file: {attachment_id}")
        print(f"Status code: {response.status_code}")
        print(response.text)

    return attachment_names_by_id, missing_file_scope


def build_attachment_names_by_contact(attachment_ids_by_contact):
    all_attachment_ids = {
        attachment_id
        for attachment_ids in attachment_ids_by_contact.values()
        for attachment_id in attachment_ids
    }

    attachment_names_by_id, missing_file_scope = fetch_attachment_names_by_id(all_attachment_ids)

    attachment_names_by_contact = defaultdict(set)
    for contact_id, attachment_ids in attachment_ids_by_contact.items():
        for attachment_id in attachment_ids:
            attachment_name = attachment_names_by_id.get(attachment_id)
            if attachment_name:
                attachment_names_by_contact[contact_id].add(attachment_name)

    return attachment_names_by_contact, missing_file_scope


def is_already_attached(row, attachment_ids_by_contact, attachment_names_by_contact):
    document_file_id = row["document_file_id"]
    contact_id = row["contact_id"]
    if document_file_id and document_file_id in attachment_ids_by_contact.get(contact_id, set()):
        return True

    normalized_filename = row["normalized_document_filename"]
    if normalized_filename and normalized_filename in attachment_names_by_contact.get(contact_id, set()):
        return True

    return False


def main():
    print("Loading document report...")

    documents_df = pd.read_excel(DOCUMENT_REPORT_FILE).copy()
    print("Documents loaded:", len(documents_df))

    documents_df["contact_id"] = documents_df["contact_id"].astype(str)
    documents_df["document_file_id"] = documents_df["document_url"].apply(extract_file_id_from_url)
    documents_df["normalized_document_filename"] = documents_df["document_filename"].apply(
        normalize_filename
    )

    unique_document_urls = set(documents_df["document_url"].dropna())
    parsed_file_id_count = documents_df["document_file_id"].notna().sum()

    print("Unique document URLs:", len(unique_document_urls))
    print("Documents with parsed file IDs:", parsed_file_id_count)

    print("Scanning attachments from HubSpot...")

    contact_ids = sorted(documents_df["contact_id"].dropna().unique())
    attachment_ids_by_contact, note_ids_by_contact = fetch_attachment_ids_by_contact(contact_ids)

    all_attachment_ids = {
        attachment_id
        for attachment_ids in attachment_ids_by_contact.values()
        for attachment_id in attachment_ids
    }
    contacts_with_notes = sum(1 for note_ids in note_ids_by_contact.values() if note_ids)
    contacts_with_attachments = sum(
        1 for attachment_ids in attachment_ids_by_contact.values() if attachment_ids
    )

    print("Contacts with notes:", contacts_with_notes)
    print("Contacts with attachments:", contacts_with_attachments)
    print("Attachments found:", len(all_attachment_ids))

    attachment_names_by_contact, missing_file_scope = build_attachment_names_by_contact(
        attachment_ids_by_contact
    )
    if missing_file_scope:
        print(
            "Attachment filename lookup skipped: the token is missing HubSpot files scopes."
        )

    print("Filtering already uploaded files...")

    direct_file_id_matches = 0
    filename_matches = 0
    already_attached_flags = []

    for row in documents_df.to_dict("records"):
        contact_attachment_ids = attachment_ids_by_contact.get(row["contact_id"], set())
        contact_attachment_names = attachment_names_by_contact.get(row["contact_id"], set())

        if row["document_file_id"] and row["document_file_id"] in contact_attachment_ids:
            direct_file_id_matches += 1
            already_attached_flags.append(True)
            continue

        if (
            row["normalized_document_filename"]
            and row["normalized_document_filename"] in contact_attachment_names
        ):
            filename_matches += 1
            already_attached_flags.append(True)
            continue

        already_attached_flags.append(False)

    documents_df["already_attached"] = already_attached_flags
    print("Documents matched by attachment file ID:", direct_file_id_matches)
    print("Documents matched by attachment filename:", filename_matches)

    filtered_df = documents_df.loc[~documents_df["already_attached"]].copy()

    print("Documents remaining after attachment filter:", len(filtered_df))

    filtered_df = filtered_df.drop_duplicates(
        subset=["contact_id", "document_file_id", "document_url"]
    )

    print("Documents after duplicate removal:", len(filtered_df))

    filtered_df = filtered_df.drop(
        columns=["document_file_id", "normalized_document_filename", "already_attached"]
    )
    filtered_df.to_excel(OUTPUT_FILE, index=False)

    print("Filtered document list saved to:", OUTPUT_FILE)
    print("Filter process complete.")


if __name__ == "__main__":
    main()
