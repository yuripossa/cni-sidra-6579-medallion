import json
from datetime import datetime, timezone
from pathlib import Path

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

SIDRA_URL = "https://apisidra.ibge.gov.br/values/t/6579/n3/all/p/all/v/all"
BRONZE_DIR = Path("data/bronze")
BRONZE_DIR.mkdir(parents=True, exist_ok=True)

@retry(wait=wait_exponential(min=1, max=20), stop=stop_after_attempt(5))
def fetch_sidra(url: str) -> list[dict]:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.json()

def main():
    batch_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    payload = fetch_sidra(SIDRA_URL)

    out = {
        "meta": {
            "source": "IBGE SIDRA API",
            "source_url": SIDRA_URL,
            "ingested_at_utc": batch_id,
            "batch_id": batch_id,
            "record_count": len(payload),
        },
        "data": payload,
    }

    out_path = BRONZE_DIR / f"sidra_6579_{batch_id}.json"
    out_path.write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")

    print(f"[OK] Bronze gerado: {out_path}")
    print(f"[OK] Registros: {len(payload)}")

if __name__ == "__main__":
    main()
