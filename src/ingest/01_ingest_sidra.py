import json
from datetime import datetime, timezone
from pathlib import Path

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

SIDRA_URL = "https://apisidra.ibge.gov.br/values/t/6579/n3/all/p/all/v/all"
# Raiz do projeto = 2 níveis acima deste arquivo: src/ingest/01_ingest_sidra.py -> (ingest) -> (src) -> (root)
import os
from pathlib import Path

# Resolve raiz do projeto de forma compatível com Databricks
def get_project_root() -> Path:
    # 1) Se existir variável de ambiente, usa
    env_root = os.getenv("PROJECT_ROOT")
    if env_root:
        return Path(env_root)

    # 2) Se __file__ existir (execução normal), usa o caminho do arquivo
    if "__file__" in globals():
        return Path(__file__).resolve().parents[2]

    # 3) Fallback Databricks: tenta achar um diretório contendo src/ e README.md subindo a partir do CWD
    cwd = Path.cwd().resolve()
    for c in [cwd] + list(cwd.parents):
        if (c / "src").exists() and (c / "README.md").exists():
            return c

    # 4) Último recurso: CWD mesmo
    return cwd

PROJECT_ROOT = get_project_root()
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
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
