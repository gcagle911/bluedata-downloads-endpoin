import os, datetime
from flask import Flask, jsonify, request
from flask_cors import CORS
from google.cloud import storage
from google.oauth2 import service_account

app = Flask(__name__)

# Allow your Shopify domain to call this API. Use "*" while testing if needed.
ALLOWED_ORIGIN = os.environ.get("SHOPIFY_ORIGIN", "*")
CORS(app, resources={r"/*": {"origins": ALLOWED_ORIGIN}})

BUCKET = os.environ.get("GCS_BUCKET", "")
EXP_HOURS = int(os.environ.get("URL_EXPIRY_HOURS", "24"))

# Service account creds (Render Secret File) OR default
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
        "tips": [
            "Use /list_daily?prefix=daily/&date=YYYY-MM-DD (if you store files in daily/DATE/...)",
            "Use /list_by_prefix?prefix=csv/2025-08-09_ (if you store files as csv/DATE_HH.csv)",
        ],
    })

# For structures like daily/YYYY-MM-DD/FILE
@app.get("/list_daily")
def list_daily():
    if not BUCKET:
        return jsonify({"error": "GCS_BUCKET not configured"}), 500

    date_str = request.args.get("date") or datetime.datetime.utcnow().strftime("%Y-%m-%d")
    prefix = request.args.get("prefix", "daily/")
    if prefix and not prefix.endswith("/"):
        prefix = prefix + "/"
    folder = f"{prefix}{date_str}/"

    contains = request.args.get("contains", "").strip()
    limit = int(request.args.get("limit", "100"))

    blobs_iter = client.list_blobs(BUCKET, prefix=folder)
    files = []
    for b in blobs_iter:
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
    return jsonify({"mode": "daily", "date": date_str, "count": len(files), "files": files[:limit]})

# ✅ For your structure: csv/YYYY-MM-DD_HH.csv (date is in filename, not in a subfolder)
@app.get("/list_by_prefix")
def list_by_prefix():
    """
    Example:
      /list_by_prefix?prefix=csv/2025-08-09_
        -> returns csv/2025-08-09_00.csv, csv/2025-08-09_08.csv, csv/2025-08-09_16.csv, etc.

      /list_by_prefix?prefix=csv/
        -> returns all files under csv/ (you can add &contains=.csv and &limit=50)

    Query params:
      - prefix=csv/2025-08-09_  (RECOMMENDED for your layout)
      - contains=.csv          (optional filter)
      - limit=20               (optional limit)
      - latest=1               (optional: if set, return only the newest one)
    """
    if not BUCKET:
        return jsonify({"error": "GCS_BUCKET not configured"}), 500

    prefix = request.args.get("prefix", "csv/")
    contains = request.args.get("contains", "").strip()
    limit = int(request.args.get("limit", "100"))
    latest_only = request.args.get("latest", "").strip() in ("1", "true", "yes")

    blobs_iter = client.list_blobs(BUCKET, prefix=prefix)

    files = []
    for b in blobs_iter:
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
            # Use updated timestamp as a proxy for "newest"
            "updated": b.updated.isoformat() if getattr(b, "updated", None) else None,
        })

    # Sort newest first (by updated time if available, else by name)
    files.sort(key=lambda x: (x["updated"] or "", x["name"]), reverse=True)

    if latest_only and files:
        files = [files[0]]

    return jsonify({
        "mode": "prefix",
        "prefix": prefix,
        "count": len(files),
        "files": files[:limit]
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)

