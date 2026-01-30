import os
from pathlib import Path
import pandas as pd

# ------------------------------
# Resolve raiz do projeto (compatível Databricks + local)
# ------------------------------
def get_project_root() -> Path:
    env_root = os.getenv("PROJECT_ROOT")
    if env_root:
        return Path(env_root)

    if "__file__" in globals():
        # src/consume/03_silver_to_gold.py -> (consume) -> (src) -> (root)
        return Path(__file__).resolve().parents[2]

    # fallback: subir a partir do CWD buscando src/ + README.md
    cwd = Path.cwd().resolve()
    for c in [cwd] + list(cwd.parents):
        if (c / "src").exists() and (c / "README.md").exists():
            return c
    return cwd


PROJECT_ROOT = get_project_root()

SILVER_DIR = PROJECT_ROOT / "data" / "silver"
GOLD_DIR = PROJECT_ROOT / "data" / "gold"
DOCS_DIR = PROJECT_ROOT / "docs"

GOLD_DIR.mkdir(parents=True, exist_ok=True)
DOCS_DIR.mkdir(parents=True, exist_ok=True)

# ------------------------------
# Helpers
# ------------------------------
def latest_silver_file() -> Path:
    print("[DEBUG] PROJECT_ROOT =", PROJECT_ROOT)
    print("[DEBUG] SILVER_DIR    =", SILVER_DIR)
    print("[DEBUG] Exists?       =", SILVER_DIR.exists())

    # Lista tudo que existe no diretório (pra provar)
    if SILVER_DIR.exists():
        all_files = list(SILVER_DIR.glob("*"))
        print("[DEBUG] Arquivos em data/silver:", [p.name for p in all_files])

    # Procura parquet
    files = sorted(SILVER_DIR.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"Nenhum parquet silver encontrado em {SILVER_DIR}")

    return files[-1]


def _safe_numeric(series: pd.Series) -> pd.Series:
    # Converte para numérico quando possível (mantém NaN se não der)
    return pd.to_numeric(series, errors="coerce")


def main():
    silver_path = latest_silver_file()
    df = pd.read_parquet(silver_path)

    print(f"[INFO] Silver: {silver_path.name}")
    print(f"[INFO] Linhas: {len(df)} | Colunas: {len(df.columns)}")
    print(f"[INFO] Primeiras colunas: {list(df.columns)[:12]}")

    # Colunas de interesse da SIDRA (padrão da API)
    # d1n = Período (nome), d3n = UF (nome), v = Valor
    # nc, nn, mc, mn... (códigos e nomes de dimensões)
    if "v" not in df.columns:
        raise ValueError("Coluna 'v' (valor) não encontrada no Silver.")

    # Normalizações
    df["v_num"] = _safe_numeric(df["v"])
    df["d3n"] = df.get("d3n", "").astype(str)  # UF (nome)
    df["d1n"] = df.get("d1n", "").astype(str)  # Período (nome)

    # Remove linhas “lixo” (ex.: cabeçalho 'Variável' ou vazio)
    # Às vezes SIDRA vem com uma linha extra "Variável"
    df = df[df["d3n"].str.strip().str.lower() != "variável"]

    # ------------------------------
    # GOLD 1: UF x Período (agregação)
    # ------------------------------
    # Se não tiver período preenchido, ainda assim agregamos só por UF
    if (df["d1n"].str.strip() == "").all():
        gold_uf_periodo = (
            df.groupby(["d3n"], dropna=False)["v_num"]
              .sum()
              .reset_index()
              .rename(columns={"d3n": "localidade", "v_num": "valor_total"})
        )
        gold_uf_periodo["periodo"] = None
        gold_uf_periodo = gold_uf_periodo[["localidade", "periodo", "valor_total"]]
    else:
        gold_uf_periodo = (
            df.groupby(["d3n", "d1n"], dropna=False)["v_num"]
              .sum()
              .reset_index()
              .rename(columns={"d3n": "localidade", "d1n": "periodo", "v_num": "valor_total"})
        )

    # ------------------------------
    # GOLD 2: Brasil x Período (agregação)
    # ------------------------------
    if (df["d1n"].str.strip() == "").all():
        gold_brasil_periodo = pd.DataFrame(
            [{"periodo": None, "valor_total": float(df["v_num"].sum())}]
        )
    else:
        gold_brasil_periodo = (
            df.groupby(["d1n"], dropna=False)["v_num"]
              .sum()
              .reset_index()
              .rename(columns={"d1n": "periodo", "v_num": "valor_total"})
        )

    # batch_id pode existir (veio do silver). Se não existir, usa o nome do arquivo.
    batch_id = None
    if "batch_id" in df.columns:
        # se tiver múltiplos, pega o mais comum
        batch_id = df["batch_id"].mode().iloc[0] if not df["batch_id"].mode().empty else None
    if not batch_id:
        batch_id = silver_path.stem.replace("sidra_6579_silver_", "")

    # ------------------------------
    # Persistência (Parquet)
    # ------------------------------
    out_uf_periodo = GOLD_DIR / f"sidra_6579_gold_uf_periodo_{batch_id}.parquet"
    out_brasil_periodo = GOLD_DIR / f"sidra_6579_gold_brasil_periodo_{batch_id}.parquet"

    gold_uf_periodo.to_parquet(out_uf_periodo, index=False)
    gold_brasil_periodo.to_parquet(out_brasil_periodo, index=False)

    print(f"[OK] Gold UF x Período: {out_uf_periodo}")
    print(f"[OK] Gold Brasil x Período: {out_brasil_periodo}")

    # ------------------------------
    # Samples (CSV) em docs/
    # ------------------------------
    sample_uf = DOCS_DIR / "gold_uf_sample.csv"
    sample_brasil = DOCS_DIR / "gold_brasil_sample.csv"

    gold_uf_periodo.head(20).to_csv(sample_uf, index=False)
    gold_brasil_periodo.head(20).to_csv(sample_brasil, index=False)

    print(f"[OK] Samples: {sample_uf.name}, {sample_brasil.name}")

    # Previews (para evidência)
    print("\n[PREVIEW] Gold UF x Período:")
    print(gold_uf_periodo.head(10).to_string(index=False))

    print("\n[PREVIEW] Gold Brasil x Período:")
    print(gold_brasil_periodo.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
