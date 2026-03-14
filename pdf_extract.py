import argparse
import re
import time
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

import pandas as pd
import requests

from config import HUBSPOT_TOKEN

PENDING_DOCUMENTS_FILE = "pending_documents.xlsx"
DOWNLOADS_DIR = "downloaded_pdfs"
RESULTS_FILE = "downloaded_pdfs_log.xlsx"
DOCUMENT_URL_COLUMN = "document_url"
DOCUMENT_FILENAME_COLUMN = "document_filename"
CONTACT_ID_COLUMN = "contact_id"
REQUEST_TIMEOUT_SECONDS = 60
REQUEST_RETRY_COUNT = 3
REQUEST_RETRY_DELAY_SECONDS = 2
REQUIRED_FILE_SCOPES = {"files", "files.ui_hidden.read"}


class HubSpotConfigurationError(RuntimeError):
    pass


def parse_args():
    parser = argparse.ArgumentParser(
        description="Download PDF files listed in pending_documents.xlsx."
    )
    parser.add_argument("--input", default=PENDING_DOCUMENTS_FILE)
    parser.add_argument("--output-dir", default=DOWNLOADS_DIR)
    parser.add_argument("--results", default=RESULTS_FILE)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--skip-existing", action="store_true")
    return parser.parse_args()


def sanitize_filename(filename):
    cleaned = re.sub(r'[<>:"/\\\\|?*]', "_", str(filename))
    cleaned = cleaned.strip().rstrip(".")
    return cleaned or "document.pdf"


def ensure_pdf_extension(filename):
    return filename if filename.lower().endswith(".pdf") else f"{filename}.pdf"


def filename_from_url(file_url):
    parsed_url = urlparse(str(file_url))
    filename = parse_qs(parsed_url.query).get("filename", [""])[0]
    return unquote(filename).strip() or None


def extract_file_id_from_url(file_url):
    parsed_url = urlparse(str(file_url))
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


def build_output_path(row, output_dir, index):
    filename = row.get(DOCUMENT_FILENAME_COLUMN) or filename_from_url(
        row.get(DOCUMENT_URL_COLUMN, "")
    )
    filename = ensure_pdf_extension(sanitize_filename(filename or f"document_{index}.pdf"))

    contact_id = str(row.get(CONTACT_ID_COLUMN, "")).strip()
    if contact_id:
        contact_dir = output_dir / sanitize_filename(contact_id)
        contact_dir.mkdir(parents=True, exist_ok=True)
        return contact_dir / filename

    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir / filename


def create_api_session():
    session = requests.Session()
    if HUBSPOT_TOKEN:
        session.headers.update(
            {
                "Authorization": f"Bearer {HUBSPOT_TOKEN}",
                "Content-Type": "application/json",
            }
        )
    return session


def request_with_retries(session, method, url, **kwargs):
    last_error = None

    for attempt in range(1, REQUEST_RETRY_COUNT + 1):
        try:
            response = session.request(method, url, timeout=REQUEST_TIMEOUT_SECONDS, **kwargs)
            return response
        except requests.exceptions.RequestException as error:
            last_error = error
            if attempt < REQUEST_RETRY_COUNT:
                time.sleep(REQUEST_RETRY_DELAY_SECONDS)

    raise last_error


def fetch_private_app_token_info(session):
    response = request_with_retries(
        session,
        "POST",
        "https://api.hubapi.com/oauth/v2/private-apps/get/access-token-info",
        json={"tokenKey": HUBSPOT_TOKEN},
    )
    response.raise_for_status()
    return response.json()


def ensure_required_file_scopes(session):
    token_info = fetch_private_app_token_info(session)
    granted_scopes = set(token_info.get("scopes", []))
    missing_scopes = sorted(REQUIRED_FILE_SCOPES - granted_scopes)
    if not missing_scopes:
        return

    missing_scope_text = ", ".join(f"`{scope}`" for scope in missing_scopes)
    raise HubSpotConfigurationError(
        "The URLs in `pending_documents.xlsx` are HubSpot file references, not public direct PDF links. "
        "To download them automatically, this private app needs the HubSpot file scopes "
        f"{missing_scope_text}. In HubSpot, open Settings > Integrations > Private Apps > this app > "
        "Scopes, add the missing scopes, click Commit changes, then rerun `python pdf_extract.py`."
    )


def fetch_signed_download_url(session, file_id):
    response = request_with_retries(
        session,
        "GET",
        f"https://api.hubapi.com/files/v3/files/{file_id}/signed-url",
    )

    if response.status_code == 403 and "MISSING_SCOPES" in response.text:
        ensure_required_file_scopes(session)

    response.raise_for_status()
    data = response.json()
    download_url = data.get("url")
    if not download_url:
        raise ValueError(f"No signed download URL returned for file ID {file_id}.")
    return download_url


def download_pdf_bytes(file_url):
    response = requests.get(file_url, timeout=REQUEST_TIMEOUT_SECONDS, allow_redirects=True)
    response.raise_for_status()

    content = response.content
    if not content:
        raise ValueError("Downloaded file is empty.")

    if not content.startswith(b"%PDF-"):
        raise ValueError(
            "Downloaded content is not a PDF. The HubSpot file link may require refreshed access."
        )

    return content


def load_pending_documents(input_file, limit=None):
    input_path = Path(input_file)
    if not input_path.exists():
        raise FileNotFoundError(f"Pending documents file not found: {input_file}")

    df = pd.read_excel(input_path)
    required_columns = {DOCUMENT_URL_COLUMN, DOCUMENT_FILENAME_COLUMN}
    missing_columns = sorted(required_columns - set(df.columns))
    if missing_columns:
        raise KeyError(f"Missing required columns: {', '.join(missing_columns)}")

    df[DOCUMENT_URL_COLUMN] = df[DOCUMENT_URL_COLUMN].astype(str).str.strip()
    df = df[df[DOCUMENT_URL_COLUMN] != ""].copy()

    if limit is not None:
        df = df.head(limit).copy()

    return df


def main():
    args = parse_args()

    print("Loading pending documents...")
    documents_df = load_pending_documents(args.input, args.limit)
    print("Documents queued for download:", len(documents_df))

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    api_session = create_api_session()
    ensure_required_file_scopes(api_session)

    results = []
    downloaded_count = 0
    skipped_count = 0
    failed_count = 0

    for index, row in enumerate(documents_df.to_dict("records"), start=1):
        file_url = row[DOCUMENT_URL_COLUMN]
        file_id = extract_file_id_from_url(file_url)
        output_path = build_output_path(row, output_dir, index)

        print(f"[{index}/{len(documents_df)}] {output_path.name}")

        if not file_id:
            failed_count += 1
            error_message = "Unable to extract a HubSpot file ID from document_url."
            print(f"  Failed: {error_message}")
            results.append(
                {
                    **row,
                    "status": "failed",
                    "local_pdf_path": str(output_path),
                    "hubspot_file_id": "",
                    "error": error_message,
                }
            )
            continue

        if args.skip_existing and output_path.exists() and output_path.stat().st_size > 0:
            skipped_count += 1
            results.append(
                {
                    **row,
                    "status": "skipped_existing",
                    "local_pdf_path": str(output_path),
                    "hubspot_file_id": file_id,
                    "error": "",
                }
            )
            continue

        try:
            signed_download_url = fetch_signed_download_url(api_session, file_id)
            pdf_bytes = download_pdf_bytes(signed_download_url)
            output_path.write_bytes(pdf_bytes)
            downloaded_count += 1
            results.append(
                {
                    **row,
                    "status": "downloaded",
                    "local_pdf_path": str(output_path),
                    "hubspot_file_id": file_id,
                    "error": "",
                }
            )
        except Exception as error:
            failed_count += 1
            results.append(
                {
                    **row,
                    "status": "failed",
                    "local_pdf_path": str(output_path),
                    "hubspot_file_id": file_id,
                    "error": str(error),
                }
            )
            print(f"  Failed: {error}")

    pd.DataFrame(results).to_excel(args.results, index=False)

    print("Downloads completed.")
    print("Downloaded:", downloaded_count)
    print("Skipped existing:", skipped_count)
    print("Failed:", failed_count)
    print("Results saved to:", args.results)
    print("PDFs saved to:", output_dir)


if __name__ == "__main__":
    try:
        main()
    except HubSpotConfigurationError as error:
        print(error)
