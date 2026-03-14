import pandas as pd

from document_filter import (
    HubSpotConnectionError,
    HubSpotConfigurationError,
    build_attachment_names_by_contact,
    extract_file_id_from_url,
    fetch_attachment_ids_by_contact,
    normalize_filename,
)

DOCUMENT_REPORT_FILE = "document_report.xlsx"
OUTPUT_FILE = "documents_to_process.xlsx"
DOCUMENT_COLUMNS = [
    "contact_id",
    "contact_name",
    "document_timestamp",
    "document_filename",
    "document_url",
]


def normalize_contact_id(value):
    if pd.isna(value):
        return None

    normalized = str(value).strip()
    return normalized or None


def main():
    print("Loading document report...")

    documents_df = pd.read_excel(DOCUMENT_REPORT_FILE, usecols=DOCUMENT_COLUMNS).copy()
    documents_df["contact_id"] = documents_df["contact_id"].map(normalize_contact_id)
    documents_df["document_file_id"] = documents_df["document_url"].map(extract_file_id_from_url)
    documents_df["normalized_document_filename"] = documents_df["document_filename"].map(
        normalize_filename
    )
    documents_df = documents_df.dropna(subset=["contact_id", "document_url"])
    documents_df = documents_df.drop_duplicates(
        subset=["contact_id", "document_url", "document_filename"]
    )

    print("Unique documents loaded:", len(documents_df))

    contact_ids = documents_df["contact_id"].drop_duplicates().tolist()
    attachment_ids_by_contact, _ = fetch_attachment_ids_by_contact(contact_ids)

    remaining_indexes = []
    contacts_needing_filename_lookup = set()

    for row in documents_df.itertuples():
        contact_attachment_ids = attachment_ids_by_contact.get(row.contact_id, set())

        if row.document_file_id and row.document_file_id in contact_attachment_ids:
            continue

        remaining_indexes.append(row.Index)

        if row.normalized_document_filename and contact_attachment_ids:
            contacts_needing_filename_lookup.add(row.contact_id)

    filtered_df = documents_df.loc[remaining_indexes].copy()

    if contacts_needing_filename_lookup:
        attachment_names_by_contact = build_attachment_names_by_contact(
            {
                contact_id: attachment_ids_by_contact[contact_id]
                for contact_id in contacts_needing_filename_lookup
                if attachment_ids_by_contact.get(contact_id)
            }
        )

        keep_mask = [
            not (
                row.normalized_document_filename
                and row.normalized_document_filename
                in attachment_names_by_contact.get(row.contact_id, set())
            )
            for row in filtered_df.itertuples(index=False)
        ]
        filtered_df = filtered_df.loc[keep_mask].copy()

    filtered_df = filtered_df.drop(columns=["document_file_id", "normalized_document_filename"])
    filtered_df.to_excel(OUTPUT_FILE, index=False)

    print("Documents remaining after attachment filter:", len(filtered_df))
    print("Filtered document list saved to:", OUTPUT_FILE)


if __name__ == "__main__":
    try:
        main()
    except HubSpotConnectionError as error:
        print(error)
    except HubSpotConfigurationError as error:
        print(error)
