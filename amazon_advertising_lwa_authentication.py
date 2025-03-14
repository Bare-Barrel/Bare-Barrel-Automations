import requests
from requests_oauthlib import OAuth2Session
import confuse
import os


# Credentials from Amazon Advertising Developer Console (Bare Barrel Ads API)
# https://developer.amazon.com/loginwithamazon/console/site/lwa/overview.html
config = confuse.Configuration('python-ad-api')
config_filename = os.path.join(config.config_dir(), 'credentials.yml')
config.set_file(config_filename)
credentials = config['Credentials'].get()

CLIENT_ID = credentials['client_id']
CLIENT_SECRET = credentials['client_secret']
REDIRECT_URI = "https://amazon.com"

# Amazon's OAuth 2 endpoints for Advertising API
# See updated urls here
# https://advertising.amazon.com/API/docs/en-us/guides/get-started/using-postman-collection
auth_grant_urls = {
    "NA": "https://www.amazon.com/ap/oa",
    "EU": "https://eu.account.amazon.com/ap/oa",
    "FE": "https://apac.account.amazon.com/ap/oa"
}

token_urls = {
    "NA": "https://api.amazon.com/auth/o2/token",
    "EU": "https://api.amazon.co.uk/auth/o2/token",
    "FE": "https://api.amazon.co.jp/auth/o2/token"
}


# Scope for Amazon Advertising; this can vary based on what you need.
# Commonly used scope is 'advertising::campaign_management'
SCOPE = ["advertising::campaign_management"]

def get_authorization_url(region):
    """
    Region: 'NA', 'EU', 'FE'

    Returns the authorization URL where the user should be directed
    to grant permission to your application.
    """
    oauth = OAuth2Session(
        client_id=CLIENT_ID,
        redirect_uri=REDIRECT_URI,
        scope=SCOPE
    )
    AUTHORIZATION_BASE_URL = auth_grant_urls[region]
    authorization_url, state = oauth.authorization_url(AUTHORIZATION_BASE_URL)
    return authorization_url, state

def exchange_code_for_token(authorization_response, region):
    """
    Exchanges the authorization code (included in the authorization_response URL)
    for an access token and refresh token.
    """
    TOKEN_URL = token_urls[region]
    oauth = OAuth2Session(client_id=CLIENT_ID, redirect_uri=REDIRECT_URI)
    token = oauth.fetch_token(
        token_url=TOKEN_URL,
        client_secret=CLIENT_SECRET,
        authorization_response=authorization_response
    )
    # token will now contain 'access_token' and 'refresh_token' (if available)
    return token


def refresh_access_token(refresh_token_value, region):
    """
    Refreshes the access token using the refresh token.
    """
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token_value,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    }
    TOKEN_URL = token_urls[region]
    response = requests.post(TOKEN_URL, data=data)
    response.raise_for_status()
    return response.json()


def main():
    # 1. Step: Get the Amazon authorization URL and direct user/browser to it.
    region = input("Please input Region code [NA (North America), EU (Europe), FE (Far East)]:")
    auth_url, state = get_authorization_url(region)
    print("Please go here and authorize the application:")
    print(auth_url)

    # 2. After granting permission, Amazon will redirect you back to your
    #    redirect URI, with a `code` parameter. Paste that full redirect URL below:
    redirected_response = input("Paste the full redirect URL here: ").strip()

    # 3. Exchange the code in the redirect URL for a token.
    token_info = exchange_code_for_token(redirected_response, region)

    # 4. You now have an access token (and possibly a refresh token).
    #    Store them securely for making subsequent requests.
    print("Access Token:", token_info.get("access_token"))
    print("Refresh Token:", token_info.get("refresh_token"))

    # 5. (Optional) Demonstrate how to refresh the token (if your access token expires)
    if token_info.get("refresh_token"):
        refreshed_tokens = refresh_access_token(token_info["refresh_token"], region)
        print("Refreshed Access Token:", refreshed_tokens.get("access_token"))

    # 5. Manually update credentials
    print("Please update credentials in /etc/python-ad-api/credentials.yml")

    # # Example: Call an Advertising API endpoint in the EU region
    # # Note: The "v2" endpoint is just an example. Your actual path may vary.
    # eu_api_url = "https://advertising-api-eu.amazon.com/v2/profiles"
    # headers = {
    #     "Authorization": f"Bearer {access_token}",
    #     "Content-Type": "application/json"
    # }
    # response = requests.get(eu_api_url, headers=headers)


if __name__ == "__main__":
    main()