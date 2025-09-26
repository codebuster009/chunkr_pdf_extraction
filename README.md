# Chunkr FastAPI Service

Minimal FastAPI wrapper around Chunkr Python SDK.

## Setup

1. Python 3.9+
2. Create venv and install deps:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

3. Set env var:

```bash
export CHUNKR_API_KEY=your_api_key
```

## Run

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8080 --reload
```

## Endpoints

- GET `/health`
- POST `/v1/process/url` with body `{ "url": "https://example.com/file.pdf" }`
- POST `/v1/process/file` multipart `file=@/path/to/file.pdf`

## Curl

```bash
curl -s http://localhost:8080/health

curl -s -X POST http://localhost:8080/v1/process/url \
  -H 'Content-Type: application/json' \
  -d '{"url":"https://example.com/sample.pdf"}'

curl -s -X POST http://localhost:8080/v1/process/file \
  -F file=@/path/to/local.pdf
```
