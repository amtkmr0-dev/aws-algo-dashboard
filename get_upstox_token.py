import os
import requests
import urllib.parse
from dotenv import load_dotenv

# Load existing environment variables
load_dotenv('keys.env')

API_KEY = os.getenv('UPSTOX_API_KEY')
API_SECRET = os.getenv('UPSTOX_API_SECRET')
REDIRECT_URI = os.getenv('UPSTOX_REDIRECT_URI')

if not all([API_KEY, API_SECRET, REDIRECT_URI]):
    print("Error: Please make sure UPSTOX_API_KEY, UPSTOX_API_SECRET, and UPSTOX_REDIRECT_URI are set in keys.env")
    exit(1)

# Step 1: Generate Login URL
encoded_redirect_uri = urllib.parse.quote(REDIRECT_URI, safe='')
login_url = f"https://api.upstox.com/v2/login/authorization/dialog?response_type=code&client_id={API_KEY}&redirect_uri={encoded_redirect_uri}"

print("="*60)
print("UPSTOX ACCESS TOKEN GENERATOR")
print("="*60)
print("\n1. Please click the link below to login to Upstox and authorize the app:")
print(f"\n{login_url}\n")
print("-" * 60)
print("2. After logging in, you will be redirected to a new URL.")
print("   The URL will look something like:")
print(f"   {REDIRECT_URI}?code=XXXXXXX")
print("\n3. Copy ONLY the code part (XXXXXXX) from the URL and paste it below.")

# Step 2: Get Auth Code from User
auth_code = input("\nEnter the auth code here: ").strip()

if not auth_code:
    print("Error: Auth code cannot be empty.")
    exit(1)

# Step 3: Fetch Access Token
print("\nFetching Access Token...")
url = 'https://api.upstox.com/v2/login/authorization/token'
headers = {
    'accept': 'application/json',
    'Content-Type': 'application/x-www-form-urlencoded',
}

data = {
    'code': auth_code,
    'client_id': API_KEY,
    'client_secret': API_SECRET,
    'redirect_uri': REDIRECT_URI,
    'grant_type': 'authorization_code',
}

try:
    response = requests.post(url, headers=headers, data=data)
    response.raise_for_status()
    token_data = response.json()
    
    access_token = token_data.get('access_token')
    
    if access_token:
        print("\n✅ Successfully retrieved Access Token!")
        
        # Open keys.env, read, find access token line, replace, or append
        with open('keys.env', 'r') as file:
            lines = file.readlines()
            
        token_found = False
        with open('keys.env', 'w') as file:
            for line in lines:
                if line.startswith('UPSTOX_ACCESS_TOKEN='):
                    file.write(f'UPSTOX_ACCESS_TOKEN={access_token}\n')
                    token_found = True
                else:
                    file.write(line)
            
            if not token_found:
                file.write(f'UPSTOX_ACCESS_TOKEN={access_token}\n')
                
        print("\n✅ Automatically saved the token to your keys.env file!")
        print("You can now run your main.py bot.")
    else:
        print("\n❌ Failed to find access_token in the response.")
        print(token_data)

except requests.exceptions.RequestException as e:
    print(f"\n❌ Error fetching token: {e}")
    if hasattr(e, 'response') and e.response is not None:
        print("Response details:", e.response.text)
