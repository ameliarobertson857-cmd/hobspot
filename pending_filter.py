from pathlib import Path
from zipfile import BadZipFile

import pandas as pd

DOCUMENTS_FILE = "documents_to_process.xlsx"
PROCESSED_LOG_FILE = "processsed_log.xlsx"
OUTPUT_FILE = "pending_documents.xlsx"

DOCUMENT_URL_COLUMN = "document_url"


def normalize_url(value):
    if pd.isna(value):
        return None

    normalized = str(value).strip()
    return normalized or None


def load_table_file(file_path):
    path = Path(file_path)

    if not path.exists():
        print(f"File not found, using empty log: {file_path}")
        return pd.DataFrame()

    try:
        return pd.read_excel(path, engine="openpyxl")
    except (ValueError, OSError, ImportError, BadZipFile):
        pass

    text = path.read_text(encoding="utf-8-sig").strip()
    if not text:
        return pd.DataFrame()

    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if lines and all("," not in line and "\t" not in line for line in lines):
        # This handles a placeholder file that contains only column names, one per line.
        return pd.DataFrame(columns=lines)

    return pd.read_csv(path, sep=None, engine="python")


def main():
    print("Loading documents to process...")
    documents_df = load_table_file(DOCUMENTS_FILE)

    if DOCUMENT_URL_COLUMN not in documents_df.columns:
        raise KeyError(f"Missing required column in {DOCUMENTS_FILE}: {DOCUMENT_URL_COLUMN}")

    print("Loading processed log...")
    processed_df = load_table_file(PROCESSED_LOG_FILE)

    documents_df[DOCUMENT_URL_COLUMN] = documents_df[DOCUMENT_URL_COLUMN].map(normalize_url)
    documents_df = documents_df.dropna(subset=[DOCUMENT_URL_COLUMN]).copy()

    if DOCUMENT_URL_COLUMN not in processed_df.columns:
        processed_df[DOCUMENT_URL_COLUMN] = None

    processed_df[DOCUMENT_URL_COLUMN] = processed_df[DOCUMENT_URL_COLUMN].map(normalize_url)
    processed_urls = set(processed_df[DOCUMENT_URL_COLUMN].dropna())

    print("Documents before processed_log filter:", len(documents_df))
    print("Already processed URLs found:", len(processed_urls))

    pending_df = documents_df[
        ~documents_df[DOCUMENT_URL_COLUMN].isin(processed_urls)
    ].copy()

    print("Documents remaining after processed_log filter:", len(pending_df))

    pending_df.to_excel(OUTPUT_FILE, index=False)
    print("Saved pending documents to:", OUTPUT_FILE)


if __name__ == "__main__":
    main()
