import json
import time

import requests

from config import HUBSPOT_TOKEN

API_BASE_URL = "https://api.hubapi.com"
REQUIRED_FILE_SCOPES = {"files", "files.ui_hidden.read"}
REQUEST_CONNECT_TIMEOUT_SECONDS = 10
REQUEST_READ_TIMEOUT_SECONDS = 30
REQUEST_RETRY_COUNT = 3
REQUEST_RETRY_DELAY_SECONDS = 2


class HubSpotConnectionError(RuntimeError):
    pass


def hubspot_request(method, url, **kwargs):
    headers = kwargs.pop("headers", {})
    merged_headers = {
        "Authorization": f"Bearer {HUBSPOT_TOKEN}",
        "Content-Type": "application/json",
        **headers,
    }
    last_error = None

    for attempt in range(1, REQUEST_RETRY_COUNT + 1):
        try:
            return requests.request(
                method,
                url,
                headers=merged_headers,
                timeout=(REQUEST_CONNECT_TIMEOUT_SECONDS, REQUEST_READ_TIMEOUT_SECONDS),
                **kwargs,
            )
        except requests.exceptions.RequestException as error:
            last_error = error
            if attempt < REQUEST_RETRY_COUNT:
                print(
                    f"HubSpot request failed ({attempt}/{REQUEST_RETRY_COUNT}) for {url}. "
                    f"Retrying in {REQUEST_RETRY_DELAY_SECONDS} seconds..."
                )
                time.sleep(REQUEST_RETRY_DELAY_SECONDS)

    raise HubSpotConnectionError(
        "Unable to reach the HubSpot API at api.hubapi.com after multiple attempts. "
        "Check your internet connection, DNS, VPN, or firewall settings and try again."
    ) from last_error


def fetch_private_app_token_info():
    response = hubspot_request(
        "POST",
        f"{API_BASE_URL}/oauth/v2/private-apps/get/access-token-info",
        json={"tokenKey": HUBSPOT_TOKEN},
    )
    response.raise_for_status()
    return response.json()


def check_endpoint(method, path, **kwargs):
    response = hubspot_request(method, f"{API_BASE_URL}/{path}", **kwargs)
    return {
        "path": path,
        "method": method,
        "status_code": response.status_code,
        "ok": response.ok,
        "reason": response.reason,
        "body": response.text[:1000],
    }


def main():
    print("=== HUBSPOT TOKEN HEALTH ===")

    token_info = fetch_private_app_token_info()
    granted_scopes = set(token_info.get("scopes", []))

    print("Hub ID:", token_info.get("hubId"))
    print("App ID:", token_info.get("appId"))
    print("User token:", token_info.get("isUserToken"))
    print("Granted scopes:")
    for scope in sorted(granted_scopes):
        print(f" - {scope}")

    missing_scopes = sorted(REQUIRED_FILE_SCOPES - granted_scopes)
    print()
    if missing_scopes:
        print("Missing file scopes:")
        for scope in missing_scopes:
            print(f" - {scope}")
    else:
        print("Required file scopes are present.")

    print()
    print("=== ENDPOINT CHECKS ===")
    checks = [
        check_endpoint("GET", "crm/v3/objects/contacts/1"),
        check_endpoint("GET", "files/v3/files/1"),
    ]

    for result in checks:
        print(f"{result['method']} {result['path']}")
        print(f"  Status: {result['status_code']} {result['reason']}")
        if not result["ok"]:
            print(f"  Body: {result['body']}")


if __name__ == "__main__":
    try:
        main()
    except HubSpotConnectionError as error:
        print(error)
