cd "$(dirname "$0")"
source .venv/bin/activate
cd .. && uvicorn app.main:app --host 0.0.0.0 --port 8000