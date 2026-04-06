from pathlib import Path

DOWNLOAD_FOLDER = Path("downloaded_pdfs")


def classify_file(path):
    with path.open("rb") as file_handle:
        header = file_handle.read(32)

    if header.startswith(b"%PDF-"):
        return "valid_pdf"
    if header.startswith(b"<!DOCTYPE html") or header.startswith(b"<html"):
        return "html_login_page"
    if not header:
        return "empty_file"
    return "unknown"


def main():
    if not DOWNLOAD_FOLDER.exists():
        print(f"Folder not found: {DOWNLOAD_FOLDER}")
        return

    pdf_paths = sorted(DOWNLOAD_FOLDER.rglob("*.pdf"))
    if not pdf_paths:
        print("No PDF files found.")
        return

    counts = {
        "valid_pdf": 0,
        "html_login_page": 0,
        "empty_file": 0,
        "unknown": 0,
    }

    for path in pdf_paths:
        status = classify_file(path)
        counts[status] += 1
        print(f"{status}: {path}")

    print()
    print("Summary:")
    for status, count in counts.items():
        print(f"{status}: {count}")


if __name__ == "__main__":
    main()
