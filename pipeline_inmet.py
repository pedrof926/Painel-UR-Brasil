# pipeline_inmet.py
# Cole aqui sua coleta/ETL do INMET e RETORNE um DataFrame com as colunas esperadas.

import pandas as pd

def build_df(attr_xlsx_path: str) -> pd.DataFrame:
    """
    Parâmetros:
      - attr_xlsx_path: caminho do arquivo data/arquivo_completo_brasil.xlsx (municípios com lat/lon)

    Retorne um DataFrame com colunas:
      CD_MUN (7 dígitos, str), NM_MUN (str), SIGLA_UF (str),
      lat (float), lon (float), data (date ou datetime), RHmin (float), RHmax (opcional)
    """
    # ============================
    # TODO: SUBSTITUA PELO SEU PIPELINE DO INMET
    # Exemplo mínimo com 3 linhas só pra subir a UI (apague quando colar o real):
    # ============================
    from datetime import datetime
    today = pd.Timestamp.today().normalize().date()
    return pd.DataFrame({
        "CD_MUN": ["5300108","3550308","3304557"],
        "NM_MUN": ["Brasília","São Paulo","Rio de Janeiro"],
        "SIGLA_UF": ["DF","SP","RJ"],
        "lat": [-15.78,-23.55,-22.90],
        "lon": [-47.93,-46.63,-43.17],
        "data": [today, today, today],
        "RHmin": [55, 35, 18],
        "RHmax": [80, 60, 40],
    })
