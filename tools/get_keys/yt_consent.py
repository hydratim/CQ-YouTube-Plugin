import os

import google_auth_oauthlib.flow

scopes = [
    "https://www.googleapis.com/auth/youtube.readonly",
    "https://www.googleapis.com/auth/yt-analytics.readonly",
]


def main():
    # Disable OAuthlib's HTTPS verification when running locally.
    # *DO NOT* leave this option enabled in production.
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

    client_secrets_file = "./YOUR_CLIENT_SECRET_FILE.json"

    # Get credentials and create an API client
    flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(
        client_secrets_file,
        scopes,
    )
    credentials = flow.run_local_server()

    with open(".env", "w") as f:
        f.writelines(
            [
                f"YT_ACCESS_TOKEN={credentials.token}\n",
                f"YT_REFRESH_TOKEN={credentials.refresh_token}\n",
                f"YT_TOKEN_URI={credentials.token_uri}\n",
                f"YT_EXPIRY={credentials.expiry.isoformat()}\n",
            ]
        )
    os.environ["YT_ACCESS_TOKEN"] = credentials.token

    print(credentials.token)


if __name__ == "__main__":
    main()
