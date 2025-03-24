import os
import json
import base64
import subprocess
from flask import Flask, request

app = Flask(__name__)

@app.route("/", methods=["POST"])
def trigger_script():
    try:
        # ✅ Ensure request contains JSON
        envelope = request.get_json(silent=True)
        if envelope is None:
            print("🚨 Error: No JSON payload received.")
            return "No JSON payload received", 400

        if "message" not in envelope:
            print("🚨 Error: Invalid Pub/Sub message format.")
            return "Invalid Pub/Sub message format", 400

        # ✅ Decode the Pub/Sub message safely
        try:
            pubsub_message = json.loads(
                base64.b64decode(envelope["message"]["data"]).decode("utf-8")
            )
        except (KeyError, json.JSONDecodeError, TypeError) as e:
            print(f"🚨 Error decoding Pub/Sub message: {str(e)}")
            return "Invalid Pub/Sub message data", 400

        # ✅ Extract filename from Pub/Sub message
        name = pubsub_message.get("name", "")
        print(f"📢 File detected: {name}")

        # ✅ Check if it's `scaled-data.parquet`
        if name.endswith("scaled-data.parquet"):
            print(f"✅ New scaled-data detected: {name}, running Prediction.py...")

            try:
                subprocess.run(["python", "Prediction.py", name], check=True)
                print("✅ Prediction.py executed successfully.")
            except subprocess.CalledProcessError as e:
                print(f"🚨 Error running Prediction.py: {e}")
                return "Prediction script failed", 500

        return "Processed", 200

    except Exception as e:
        print(f"🚨 Unexpected Error: {str(e)}")
        return "Internal Server Error", 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
