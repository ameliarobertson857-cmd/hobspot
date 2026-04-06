import argparse
import json
from pathlib import Path

import pandas as pd

from pdf_extract import (
    HubSpotConfigurationError,
    create_api_session,
    download_pdf_bytes,
    ensure_pdf_extension,
    ensure_required_file_scopes,
    extract_file_id_from_url,
    fetch_signed_download_url,
    is_valid_pdf_file,
    load_pending_documents,
    sanitize_filename,
)

DEFAULT_INPUT_FILES = ("pending_documents.xlsx", "documents_to_process.xlsx")
DOWNLOAD_FOLDER = "downloaded_pdfs"
RESULTS_FILE = "downloaded_pdfs_log.xlsx"
PROCESSED_PDFS_FILE = "vec_processed_pdfs.json"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Download the remaining pending PDFs and skip files already downloaded."
    )
    parser.add_argument("--input", default=None)
    parser.add_argument("--output-dir", default=DOWNLOAD_FOLDER)
    parser.add_argument("--results", default=RESULTS_FILE)
    parser.add_argument("--limit", type=int, default=None)
    return parser.parse_args()


def resolve_input_file(explicit_input):
    if explicit_input:
        return explicit_input

    for candidate in DEFAULT_INPUT_FILES:
        if Path(candidate).exists():
            return candidate

    return DEFAULT_INPUT_FILES[0]


def build_flat_output_path(row, output_dir):
    filename = row.get("document_filename")
    safe_filename = ensure_pdf_extension(sanitize_filename(filename))
    return output_dir / safe_filename


def classify_existing_file(file_path):
    path = Path(file_path)
    if not path.exists():
        return "missing"
    if not path.stat().st_size:
        return "empty_file"
    if is_valid_pdf_file(path):
        return "valid_pdf"

    with path.open("rb") as file_handle:
        header = file_handle.read(32)

    if header.startswith(b"<!DOCTYPE html") or header.startswith(b"<html"):
        return "html_login_page"
    return "invalid_file"


def load_processed_pdf_names(processed_file=PROCESSED_PDFS_FILE):
    registry_path = Path(processed_file)
    if not registry_path.exists():
        return set()

    try:
        data = json.loads(registry_path.read_text(encoding="utf-8"))
    except Exception:
        return set()

    if isinstance(data, dict):
        records = data.get("processed_pdfs", [])
    elif isinstance(data, list):
        records = data
    else:
        records = []

    processed_names = set()
    for record in records:
        filename = str(record.get("source_pdf_filename") or "").strip()
        if filename:
            processed_names.add(filename.casefold())

        archived_path = str(record.get("archived_pdf_path") or "").strip()
        if archived_path:
            processed_names.add(Path(archived_path).name.casefold())

    return processed_names


def main():
    args = parse_args()

    input_file = resolve_input_file(args.input)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print("Loading pending documents...")
    print("Input file:", input_file)
    documents_df = load_pending_documents(input_file, args.limit)
    print("Documents queued:", len(documents_df))

    if documents_df.empty:
        print("No pending documents found.")
        raise SystemExit

    api_session = create_api_session()
    ensure_required_file_scopes(api_session)
    processed_names = load_processed_pdf_names()

    results = []
    downloaded_count = 0
    skipped_count = 0
    failed_count = 0

    for index, row in enumerate(documents_df.to_dict("records"), start=1):
        file_url = row["document_url"]
        file_id = extract_file_id_from_url(file_url)
        output_path = build_flat_output_path(row, output_dir)

        print(f"[{index}/{len(documents_df)}] {output_path.name}")

        if output_path.name.casefold() in processed_names:
            skipped_count += 1
            results.append(
                {
                    **row,
                    "status": "skipped_processed_upload",
                    "local_pdf_path": str(output_path),
                    "hubspot_file_id": file_id or "",
                    "error": "",
                }
            )
            continue

        existing_status = classify_existing_file(output_path)
        if existing_status == "valid_pdf":
            skipped_count += 1
            results.append(
                {
                    **row,
                    "status": "skipped_existing_valid",
                    "local_pdf_path": str(output_path),
                    "hubspot_file_id": file_id or "",
                    "error": "",
                }
            )
            continue

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

        if existing_status in {"html_login_page", "invalid_file", "empty_file"}:
            print(f"  Replacing existing {existing_status}.")

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
    print("Skipped existing valid PDFs:", skipped_count)
    print("Failed:", failed_count)
    print("Results saved to:", args.results)
    print("PDFs saved to:", output_dir)


if __name__ == "__main__":
    try:
        main()
    except HubSpotConfigurationError as error:
        print(error)
