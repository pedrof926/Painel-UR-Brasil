# Painel de Umidade Relativa – Brasil (INMET)

Web service (Render Free) que roda **pipeline + Dash** em um único app:
- coleta a previsão (INMET) on-the-fly
- cache por dia (fuso America/Sao_Paulo)
- mapa por classe, gráfico 5 dias, cards e listagem por classificação

## Como usar

1) Cole sua rotina do **INMET** em `pipeline_inmet.py` (função `build_df`) retornando um `DataFrame` com colunas:
   `CD_MUN, NM_MUN, SIGLA_UF, lat, lon, data, RHmin` (e opcional `RHmax`).

2) Garanta o arquivo `data/arquivo_completo_brasil.xlsx` no repo (lat/lon por município).

3) Local:
   ```bash
   pip install -r requirements.txt
   python app.py
