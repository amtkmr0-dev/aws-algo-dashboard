import asyncio
import websockets
import json

async def fetch_data():
    uri = "ws://65.1.147.104:8080/ws_nifty"
    try:
        async with websockets.connect(uri) as websocket:
            data_str = await websocket.recv()
            data = json.loads(data_str)
            print("Successfully received data.")
            with open("ws_data_nifty.json", "w") as f:
                json.dump(data, f, indent=2)
    except Exception as e:
        print(f"Error connecting: {e}")

if __name__ == "__main__":
    asyncio.run(fetch_data())
