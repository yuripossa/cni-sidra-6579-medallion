from pathlib import Path
import pandas as pd

SILVER_DIR = Path("data/silver")
GOLD_DIR = Path("data/gold")
DOCS_DIR = Path("docs")

GOLD_DIR.mkdir(parents=True, exist_ok=True)
DOCS_DIR.mkdir(parents=True, exist_ok=True)

def latest_silver_file() -> Path:
    files = sorted(SILVER_DIR.glob("sidra_6579_silver_*.parquet"))
    if not files:
        raise FileNotFoundError("Nenhum parquet silver encontrado em data/silver")
    return files[-1]

def main():
    silver_path = latest_silver_file()
    df = pd.read_parquet(silver_path)
    # Remove linha de cabeçalho/metadata que pode vir como registro
    df["d3n"] = df["d3n"].astype(str)
    df = df[df["d3n"].str.lower() != "variável"]


    print(f"[INFO] Silver: {silver_path.name}")
    print(f"[INFO] Linhas: {len(df)} | Colunas: {len(df.columns)}")
    print("[INFO] Colunas:", list(df.columns))

    # Para o seu retorno do SIDRA:
    # d3n -> nome da variável (ex.: "População residente estimada")
    # d1n -> UF/Localidade (Acre, Alagoas, ...)
    # v   -> valor
    col_variavel = "d3n"
    col_uf = "d1n"
    col_valor = "v"

    for c in [col_variavel, col_uf, col_valor]:
        if c not in df.columns:
            raise ValueError(f"Coluna esperada não encontrada: {c}. Colunas: {list(df.columns)}")

    df[col_valor] = pd.to_numeric(df[col_valor], errors="coerce")

    batch_id = df["batch_id"].iloc[0] if "batch_id" in df.columns else "unknown"

    # GOLD 1: População por UF (e variável)
    gold_uf = (
        df.groupby([col_variavel, col_uf], dropna=False)[col_valor]
          .sum()
          .reset_index()
          .rename(columns={
              col_variavel: "variavel",
              col_uf: "uf",
              col_valor: "valor_total"
          })
          .sort_values(["variavel", "uf"])
    )

    out_uf = GOLD_DIR / f"sidra_6579_gold_uf_{batch_id}.parquet"
    gold_uf.to_parquet(out_uf, index=False)

    # GOLD 2: Total Brasil (soma geral por variável)
    gold_brasil = (
        df.groupby([col_variavel], dropna=False)[col_valor]
          .sum()
          .reset_index()
          .rename(columns={col_variavel: "variavel", col_valor: "valor_total"})
          .sort_values(["variavel"])
    )

    out_br = GOLD_DIR / f"sidra_6579_gold_brasil_{batch_id}.parquet"
    gold_brasil.to_parquet(out_br, index=False)

    # Evidências para versionar
    sample_uf = DOCS_DIR / "gold_uf_sample.csv"
    sample_br = DOCS_DIR / "gold_brasil_sample.csv"
    gold_uf.head(60).to_csv(sample_uf, index=False)
    gold_brasil.to_csv(sample_br, index=False)

    print(f"[OK] Gold UF: {out_uf}")
    print(f"[OK] Gold Brasil: {out_br}")
    print(f"[OK] Samples: {sample_uf.name}, {sample_br.name}")

    print("\n[PREVIEW] Gold UF:")
    print(gold_uf.head(10))

    print("\n[PREVIEW] Gold Brasil:")
    print(gold_brasil)

if __name__ == "__main__":
    main()
