import json
from pathlib import Path
import pandas as pd

BRONZE_DIR = Path("data/bronze")
SILVER_DIR = Path("data/silver")
SILVER_DIR.mkdir(parents=True, exist_ok=True)

def latest_bronze_file() -> Path:
    files = sorted(BRONZE_DIR.glob("sidra_6579_*.json"))
    if not files:
        raise FileNotFoundError("Nenhum arquivo bronze encontrado em data/bronze")
    return files[-1]

def main():
    bronze_path = latest_bronze_file()
    raw = json.loads(bronze_path.read_text(encoding="utf-8"))

    meta = raw["meta"]
    rows = raw["data"]  # lista de dicts

    df = pd.DataFrame(rows)

    # Padronizar nomes de colunas (snake_case simples)
    df.columns = (
        df.columns.str.strip()
                 .str.lower()
                 .str.replace(" ", "_")
                 .str.replace("-", "_")
    )

    # Adicionar colunas técnicas
    df["batch_id"] = meta["batch_id"]
    df["ingested_at_utc"] = meta["ingested_at_utc"]
    df["source_url"] = meta["source_url"]

    # Tipagem: tenta converter tudo que parecer numérico
    # (SIDRA retorna muito como texto)
    for c in df.columns:
        if c in ["batch_id", "ingested_at_utc", "source_url"]:
            continue
        df[c] = pd.to_numeric(df[c], errors="ignore")

    # Salvar em parquet
    out_path = SILVER_DIR / f"sidra_6579_silver_{meta['batch_id']}.parquet"
    df.to_parquet(out_path, index=False)

    print(f"[OK] Silver gerado: {out_path}")
    print(f"[OK] Linhas: {len(df)} | Colunas: {len(df.columns)}")
    print("[OK] Amostra de colunas:", list(df.columns)[:12])

if __name__ == "__main__":
    main()
