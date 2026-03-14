from document_filter import (
    HubSpotConnectionError,
    fetch_attachment_ids_by_note,
    fetch_note_ids_by_contact,
)
import pandas as pd

# Configuration
DOCUMENT_REPORT_FILE = "document_report.xlsx"
MAX_CONTACTS_TO_PROCESS = None  # Set to None to process all contacts

def main():
    print("Loading document report...")
    documents_df = pd.read_excel(DOCUMENT_REPORT_FILE)
    print(f"Documents loaded: {len(documents_df)}")

    # Get unique contact IDs
    all_contact_ids = documents_df["contact_id"].unique()
    print(f"Total unique contacts found: {len(all_contact_ids)}")

    # Limit for testing if specified
    contact_ids_to_process = all_contact_ids
    if MAX_CONTACTS_TO_PROCESS is not None:
        contact_ids_to_process = all_contact_ids[:MAX_CONTACTS_TO_PROCESS]
        print(f"Limited to first {MAX_CONTACTS_TO_PROCESS} contacts for testing")

    print(f"Processing {len(contact_ids_to_process)} contacts...")

    total_notes = 0
    total_attachments = 0
    contacts_with_notes = 0
    contacts_with_attachments = 0

    for i, contact_id in enumerate(contact_ids_to_process, 1):
        contact_id_str = str(contact_id).strip()
        print(f"\n[{i}/{len(contact_ids_to_process)}] Checking contact: {contact_id_str}")

        try:
            note_ids_by_contact = fetch_note_ids_by_contact([contact_id_str])
            note_ids = sorted(note_ids_by_contact.get(contact_id_str, set()))

            print(f"  Associated notes found: {len(note_ids)}")
            total_notes += len(note_ids)

            if note_ids:
                contacts_with_notes += 1
                print(f"  Note IDs: {', '.join(note_ids[:10])}{'...' if len(note_ids) > 10 else ''}")

                attachment_ids_by_note = fetch_attachment_ids_by_note(note_ids)

                attached_file_ids = sorted(
                    {
                        attachment_id
                        for attachment_ids in attachment_ids_by_note.values()
                        for attachment_id in attachment_ids
                    }
                )

                print(f"  Attached file IDs found: {len(attached_file_ids)}")
                total_attachments += len(attached_file_ids)

                if attached_file_ids:
                    contacts_with_attachments += 1
                    print(f"  Total attachments: {', '.join(attached_file_ids[:5])}{'...' if len(attached_file_ids) > 5 else ''}")

                    # Show details per note
                    for note_id in note_ids:
                        attachment_ids = sorted(attachment_ids_by_note.get(note_id, set()))
                        if attachment_ids:
                            print(f"    Note {note_id} attachments: {', '.join(attachment_ids)}")
                else:
                    print("  No note attachments found for this contact.")
            else:
                print("  No associated notes found for this contact.")

        except Exception as e:
            print(f"  Error processing contact {contact_id_str}: {e}")

    print("=== SUMMARY ===")
    print(f"Total contacts processed: {len(contact_ids_to_process)}")
    print(f"Contacts with notes: {contacts_with_notes}")
    print(f"Contacts with attachments: {contacts_with_attachments}")
    print(f"Total notes found: {total_notes}")
    print(f"Total attachments found: {total_attachments}")

if __name__ == "__main__":
    try:
        main()
    except HubSpotConnectionError as error:
        print(error)
