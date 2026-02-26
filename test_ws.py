import os
from dotenv import load_dotenv
import upstox_client
from upstox_client.rest import ApiException
from upstox_client.websocket.market_data import MarketDataStreamer

load_dotenv("keys.env", override=True)
access_token = os.getenv("UPSTOX_ACCESS_TOKEN")

print("AccessToken loaded:", bool(access_token))

def on_message(message):
    print("Update received:", message)

def on_error(error):
    print("Error:", error)

def on_close():
    print("Connection closed")

def on_open():
    print("Connection opened, subscribing...")
    streamer.subscribe(["NSE_EQ|INE040A01034"], "full")
    print("Subscribed to HDFC Bank")

try:
    upstox_client.configuration.auth_settings = lambda: {"OAUTH2": {"in": "header", "type": "oauth2", "key": access_token}}
    streamer = MarketDataStreamer(
        upstox_client.ApiClient(upstox_client.Configuration()),
        ["NSE_EQ|INE040A01034"],
        "full"
    )
    
    streamer.on('message', on_message)
    streamer.on('error', on_error)
    streamer.on('close', on_close)
    streamer.on('open', on_open)
    
    print("Connecting streamer...")
    streamer.connect()
    
except Exception as e:
    print("Exception when setting up websocket:", str(e))
