import os, datetime
from flask import Flask, jsonify, request
from flask_cors import CORS
from google.cloud import storage
from google.oauth2 import service_account

app = Flask(__name__)

# Allow your Shopify domain to call this API. While testing you can set "*" (not recommended long term).
ALLOWED_ORIGIN = os.environ.get("SHOPIFY_ORIGIN", "*")
CORS(app, resources={r"/*": {"origins": ALLOWED_ORIGIN}})

BUCKET = os.environ.get("GCS_BUCKET", "")
EXP_HOURS = int(os.environ.get("URL_EXPIRY_HOURS", "24"))

# Service account key file path (Render Secret File) OR default creds
creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
if creds_path and os.path.exists(creds_path):
    creds = service_account.Credentials.from_service_account_file(creds_path)
    client = storage.Client(credentials=creds)
else:
    client = storage.Client()

def signed_url(blob_name, hours=EXP_HOURS):
    bucket = client.bucket(BUCKET)
    blob = bucket.blob(blob_name)
    return blob.generate_signed_url(
        version="v4",
        expiration=datetime.timedelta(hours=hours),
        method="GET",
    )

@app.get("/health")
def health():
    return jsonify({"ok": True})

@app.get("/")
def root():
    return jsonify({
        "ok": True,
        "message": "Bluedata downloads endpoint",
        "endpoints": ["/list?prefix=daily/&date=YYYY-MM-DD"]
    })

@app.get("/list")
def list_files():
    """
    Returns signed links for files in your bucket by day.

    Query params:
      - date=YYYY-MM-DD   (default: today UTC)
      - prefix=path/      (default: "daily/")
      - contains=BTC      (optional: only return names containing this text)
    Folder convention: {prefix}{date}/
      e.g., daily/2025-08-27/
    """
    if not BUCKET:
        return jsonify({"error": "GCS_BUCKET not configured"}), 500

    # Date handling (UTC)
    date_str = request.args.get("date")
    if not date_str:
        date_str = datetime.datetime.utcnow().strftime("%Y-%m-%d")

    # Prefix (folder root)
    prefix = request.args.get("prefix", "daily/")
    if prefix and not prefix.endswith("/"):
        prefix = prefix + "/"

    folder = f"{prefix}{date_str}/"

    # Optional text filter (e.g., 'BTC', 'ETH', '.csv')
    contains = request.args.get("contains", "").strip()

    try:
        blobs_iter = client.list_blobs(BUCKET, prefix=folder)
        files = []
        for b in blobs_iter:
            # skip "directory markers"
            if b.name.endswith("/"):
                continue
            name = b.name.split("/")[-1]
            if contains and contains not in name:
                continue
            files.append({
                "name": name,
                "path": b.name,
                "size_bytes": b.size or 0,
                "signed_url": signed_url(b.name),
            })

        files.sort(key=lambda x: x["name"])
        return jsonify({"date": date_str, "count": len(files), "files": files})

    except Exception as e:
        return jsonify({"error": str(e), "date": date_str, "prefix": prefix}), 500

if __name__ == "__main__":
    # Local run: flask --app app run -p 8000
    app.run(host="0.0.0.0", port=8000)
