import json
import os
from datetime import datetime, timedelta
from time import sleep

import requests
from dotenv import load_dotenv


def main():
    client_secrets_file = "./YOUR_CLIENT_SECRET_FILE.json"
    load_dotenv()
    # refresh credentials
    token_uri = os.environ.get("YT_TOKEN_URI")
    expiry = datetime.fromisoformat(os.environ.get("YT_EXPIRY"))
    refresh_token = os.environ.get("YT_REFRESH_TOKEN")
    while True:
        if expiry - timedelta(seconds=1) > datetime.now():
            remaining = (expiry - timedelta(seconds=1) - datetime.now()).total_seconds()
            print(f"sleeping for: {remaining}")
            sleep(remaining)
        print("refreshing credentials")
        # client credentials
        with open(client_secrets_file) as f:
            jdata = json.load(f)
        client_secret = jdata["installed"]["client_secret"]
        client_id = jdata["installed"]["client_id"]
        response = requests.post(
            token_uri,
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_secret": client_secret,
                "client_id": client_id,
            },
        )
        credentials = response.json()
        print(expiry.isoformat())
        expiry = datetime.now() + timedelta(seconds=credentials["expires_in"])
        print(expiry.isoformat())

        with open(".env", "w") as f:
            f.write(
                f"YT_ACCESS_TOKEN={credentials['access_token']}\n"
                f"YT_REFRESH_TOKEN={refresh_token}\n"
                f"YT_TOKEN_URI={token_uri}\n"
                f"YT_EXPIRY={expiry.isoformat()}\n"
            )
        print("success")


if __name__ == "__main__":
    main()
