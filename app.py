# Painel de Umidade Relativa – Brasil (UR mínima, 5 dias)


import os, json, time
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dash import Dash, dcc, html, Input, Output, State
import plotly.express as px
import plotly.graph_objects as go
import pytz
from flask import request, jsonify
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================== CONFIG ==================
APP_TITLE   = "Umidade Relativa do Ar – Brasil (UR mínima)"
BRT         = pytz.timezone("America/Sao_Paulo")

# Tudo na RAIZ do repo
ATTR_XLSX    = os.environ.get("ATTR_XLSX", "arquivo_completo_brasil.xlsx")   # municípios (lat/lon)
GEOJSON_PATH = os.environ.get("GEOJSON_PATH", "municipios_br.geojson")       # geojson de municípios (opcional)
REFRESH_TOKEN= os.environ.get("REFRESH_TOKEN", "")                           # opcional /refresh?token=...
# Para testes no plano free (primeiro deploy) você pode limitar a quantidade processada:
MAX_MUN      = int(os.environ.get("MAX_MUN", "0"))  # 0 = todos; ex: 200 para testar mais rápido

# API do INMET (previsão por município IBGE)
# Em geral o endpoint é: https://apiprevmet3.inmet.gov.br/previsao/{CD_MUN}
INMET_FORECAST_URL = os.environ.get(
    "INMET_FORECAST_URL_TEMPLATE",
    "https://apiprevmet3.inmet.gov.br/previsao/{ibge}"
)

REQUEST_TIMEOUT = float(os.environ.get("REQUEST_TIMEOUT", "8"))  # segundos
MAX_WORKERS     = int(os.environ.get("MAX_WORKERS", "16"))       # threads

# ================== CLASSES/CORES ==================
COLOR_MAP = {
    "Ideal (>60%)":            "#1E3A8A",  # azul escuro
    "Quase ideal (41–60%)":    "#60A5FA",  # azul claro
    "Observação (30–40%)":     "#FEF08A",  # amarelo
    "Atenção (20–29%)":        "#F59E0B",  # laranja
    "Caso de alerta (12–19%)": "#F87171",  # vermelho claro
    "Emergência (<12%)":       "#B91C1C",  # vermelho forte
}
CLASS_ORDER = [
    "Ideal (>60%)",
    "Quase ideal (41–60%)",
    "Observação (30–40%)",
    "Atenção (20–29%)",
    "Caso de alerta (12–19%)",
    "Emergência (<12%)",
]

def classificar_rhmin(v):
    if pd.isna(v): return np.nan
    v = float(v)
    if v > 60:            return "Ideal (>60%)"
    if 41 <= v <= 60:     return "Quase ideal (41–60%)"
    if 30 <= v <= 40:     return "Observação (30–40%)"
    if 20 <= v <= 29:     return "Atenção (20–29%)"
    if 12 <= v <= 19:     return "Caso de alerta (12–19%)"
    if 0  <= v < 12:      return "Emergência (<12%)"
    return np.nan

# ================== GEOJSON ==================
def _guess_cd_mun(props: dict) -> str | None:
    for k in ["CD_MUN","CD_GEOCMU","CD_GEOCODI","CD_MUNIC","CD_IBGE","GEOCODIGO","GEOCODE","GEOCOD_M","codigo_ibge"]:
        if k in props and pd.notna(props[k]):
            s = str(props[k]); dig = "".join(ch for ch in s if ch.isdigit())
            if 6 <= len(dig) <= 7:
                return dig.zfill(7)
    for v in props.values():
        s = str(v); dig = "".join(ch for ch in s if ch.isdigit())
        if 6 <= len(dig) <= 7:
            return dig.zfill(7)
    return None

def load_geojson(path):
    if not path or not os.path.exists(path):
        print("[geo] GeoJSON não encontrado, usando fallback por pontos (lat/lon).")
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            gj = json.load(f)
        for ft in gj.get("features", []):
            props = ft.setdefault("properties", {})
            cd = _guess_cd_mun(props)
            props["CD_MUN"] = cd or str(props.get("CD_MUN","")).zfill(7)
        return gj
    except Exception as e:
        print("[geo] Falha ao ler GeoJSON:", e)
        return None

gj = load_geojson(GEOJSON_PATH)

# ================== PIPELINE INMET ==================
def load_attr_municipios(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise SystemExit(f"❌ Não encontrei o arquivo de municípios: {path}")
    attr = pd.read_excel(path, engine="openpyxl")
    cols_low = {c.lower(): c for c in attr.columns}
    cd = cols_low.get("cd_mun") or cols_low.get("cd_ibge") or "CD_MUN"
    nm = cols_low.get("nm_mun") or "NM_MUN"
    uf = cols_low.get("sigla_uf") or cols_low.get("uf") or "SIGLA_UF"
    lat = next((c for c in attr.columns if "lat" in c.lower()), None)
    lon = next((c for c in attr.columns if "lon" in c.lower()), None)
    if not lat or not lon:
        raise SystemExit("❌ Base de municípios sem colunas de latitude/longitude.")
    out = attr.rename(columns={cd:"CD_MUN", nm:"NM_MUN", uf:"SIGLA_UF", lat:"lat", lon:"lon"})
    out["CD_MUN"] = out["CD_MUN"].astype(str).str.extract(r"(\d+)")[0].str.zfill(7)
    out = out.dropna(subset=["lat","lon"]).drop_duplicates("CD_MUN")
    return out[["CD_MUN","NM_MUN","SIGLA_UF","lat","lon"]]

def _parse_inmet_resp(ibge: str, j: dict) -> dict:
    """
    Tenta interpretar respostas do INMET para previsão diária com umidade.
    Padrões conhecidos:
      - { "<ibge>": { "YYYY-MM-DD": {"umidade_min": 28, ...}, ... } }
      - { "YYYY-MM-DD": {"umidade_min": 28, ...}, ... }
    Aceita variações de chave: umidade_min | ur_min | umi_min
    Retorna: {date -> rhmin(float)}
    """
    def _get_min(d):
        for k in ["umidade_min","ur_min","umi_min","umidadeMin","UR_min"]:
            if isinstance(d, dict) and k in d:
                try:
                    return float(d[k])
                except Exception:
                    pass
        # fallback: se vier lista de horários com "umidade", pega o mínimo
        if isinstance(d, dict):
            for v in d.values():
                if isinstance(v, (list, tuple)) and v and isinstance(v[0], (int, float, str)):
                    try:
                        vals = pd.to_numeric(pd.Series(v), errors="coerce")
                        if vals.notna().any():
                            return float(np.nanmin(vals))
                    except Exception:
                        pass
        return np.nan

    # Se vier com a chave do IBGE no topo
    if isinstance(j, dict) and ibge in j and isinstance(j[ibge], dict):
        j = j[ibge]

    out = {}
    if isinstance(j, dict):
        for k, v in j.items():
            # k pode ser a data
            try:
                d = pd.to_datetime(k).date()
            except Exception:
                continue
            out[d] = _get_min(v)
    return out

def _fetch_one(ibge: str) -> dict:
    url = INMET_FORECAST_URL.format(ibge=ibge)
    r = requests.get(url, timeout=REQUEST_TIMEOUT)
    if r.status_code != 200:
        return {}
    try:
        j = r.json()
    except Exception:
        return {}
    return _parse_inmet_resp(ibge, j)

def build_df(attr_xlsx_path: str) -> pd.DataFrame:
    """
    Coleta previsão do INMET sem XLSX intermediário e retorna:
    [CD_MUN, NM_MUN, SIGLA_UF, lat, lon, data, RHmin]
    - Usa INMET_FORECAST_URL (por IBGE)
    - Limita a 5 dias a partir de hoje
    """
    attr = load_attr_municipios(attr_xlsx_path)
    if MAX_MUN > 0:
        attr = attr.head(MAX_MUN).copy()  # útil para o primeiro deploy no plano free

    today = datetime.now(BRT).date()
    target_days = [today + timedelta(days=i) for i in range(5)]
    rows = []

    # paraleliza para acelerar, com limites razoáveis
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(_fetch_one, ibge): (ibge, row)
                   for ibge, row in zip(attr["CD_MUN"], attr.to_dict("records"))}
        for fut in as_completed(futures):
            ibge, row = futures[fut]
            try:
                day_map = fut.result()  # {date -> rhmin}
            except Exception:
                day_map = {}
            for d in target_days:
                rh = day_map.get(d, np.nan)
                rows.append({
                    "CD_MUN": row["CD_MUN"],
                    "NM_MUN": row["NM_MUN"],
                    "SIGLA_UF": row["SIGLA_UF"],
                    "lat": row["lat"],
                    "lon": row["lon"],
                    "data": d,
                    "RHmin": rh,
                    "RHmax": np.nan
                })
            # pequena pausa de gentileza a cada ~200 requisições concluídas
            if len(rows) % 1000 == 0:
                time.sleep(0.2)

    df = pd.DataFrame(rows)
    # Se por algum motivo não retornou nada (API fora), evitamos quebrar o app
    if df["RHmin"].notna().sum() == 0:
        # marca NaN, o app continua de pé (e você pode forçar /refresh quando a API voltar)
        print("[inmet] Aviso: sem dados de umidade na resposta. Mantendo NaN (app não cai).")
    return df

# ================== CACHE DIÁRIO (BRT) ==================
_CACHE = {"key": None, "df": None}

def _sanitize_df(df: pd.DataFrame) -> pd.DataFrame:
    need = ["CD_MUN","NM_MUN","SIGLA_UF","lat","lon","data","RHmin"]
    miss = [c for c in need if c not in df.columns]
    if miss:
        raise SystemExit(f"❌ Pipeline INMET sem colunas: {', '.join(miss)}")
    df = df.copy()
    df["CD_MUN"] = df["CD_MUN"].astype(str).str.extract(r"(\d+)")[0].str.zfill(7)
    df["data"]   = pd.to_datetime(df["data"], errors="coerce").dt.date
    for c in ["lat","lon","RHmin"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    if "RHmax" in df.columns:
        df["RHmax"] = pd.to_numeric(df["RHmax"], errors="coerce")
    return df.sort_values(["CD_MUN","data"]).reset_index(drop=True)

def _demo_df() -> pd.DataFrame:
    base = pd.DataFrame({
        "CD_MUN": ["5300108","3550308","3304557"],
        "NM_MUN": ["Brasília","São Paulo","Rio de Janeiro"],
        "SIGLA_UF": ["DF","SP","RJ"],
        "lat": [-15.78,-23.55,-22.90],
        "lon": [-47.93,-46.63,-43.17],
    })
    today = datetime.now(BRT).date()
    rows=[]
    vals = [55, 35, 18, 62, 28]
    for _, r in base.iterrows():
        for i in range(5):
            d = today + timedelta(days=i)
            rows.append({**r.to_dict(), "data": d, "RHmin": vals[i % 5], "RHmax": np.nan})
    return pd.DataFrame(rows)

def get_data(force=False) -> pd.DataFrame:
    key = datetime.now(BRT).strftime("%Y-%m-%d")
    if (not force) and _CACHE["key"] == key and _CACHE["df"] is not None:
        return _CACHE["df"]
    print(f"[umidade] (re)montando dados – chave diária {key}")
    try:
        df = build_df(ATTR_XLSX)
    except Exception as e:
        print(">>> AVISO: falha no pipeline do INMET, usando dados demo. Erro:", e)
        df = _demo_df()
    df = _sanitize_df(df)
    _CACHE.update(key=key, df=df)
    return df

# ================== APP (Dash) ==================
app = Dash(__name__)
server = app.server
app.title = APP_TITLE

@server.route("/refresh", methods=["GET","POST"])
def refresh_endpoint():
    if REFRESH_TOKEN:
        token = request.args.get("token","")
        if token != REFRESH_TOKEN:
            return ("forbidden", 403)
    try:
        get_data(force=True)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

umid = get_data()
ufs  = sorted(umid["SIGLA_UF"].dropna().unique().tolist())

def current_dates():
    return sorted(pd.Series(get_data()["data"]).dropna().unique().tolist())

def pick_default_cd(df: pd.DataFrame):
    m = df["NM_MUN"].str.lower() == "brasília"
    if m.any(): return str(df.loc[m, "CD_MUN"].iloc[0]).zfill(7)
    return str(df["CD_MUN"].iloc[0]).zfill(7)

store_sel = dcc.Store(id="muni-sel", data=pick_default_cd(umid))

# -------- Controles
controls = html.Div([
    html.Div([
        html.Label("Estado (UF)"),
        dcc.Dropdown(id="uf-filter",
                     options=[{"label": u, "value": u} for u in ufs],
                     value=[], multi=True, placeholder="Selecionar UF(s)"),
    ], style={"minWidth":"220px","flex":"1","marginRight":"8px"}),
    html.Div([
        html.Label("Município"),
        dcc.Dropdown(id="muni-filter", options=[], value=None, multi=False, placeholder="Selecionar município"),
    ], style={"minWidth":"320px","flex":"2","marginRight":"8px"}),
    html.Div([
        html.Label("Data do mapa"),
        dcc.Slider(id="date-slider",
                   min=0, max=max(len(current_dates())-1,0), step=1,
                   value=max(len(current_dates())-1,0),
                   marks={i: pd.to_datetime(d).strftime("%d/%m") for i, d in enumerate(current_dates())})
    ], style={"minWidth":"320px","flex":"3"}),
], style={"display":"flex","gap":"12px","flexWrap":"wrap","marginBottom":"8px"})

# -------- Layout principal
two_cols = html.Div([
    html.Div([dcc.Graph(id="mapa", style={"height":"70vh"})], style={"flex":"1","paddingRight":"8px"}),
    html.Div([
        dcc.Graph(id="grafico", style={"height":"54vh","marginBottom":"12px"}),
        html.Div(id="cards-ur", style={"display":"flex","gap":"12px","justifyContent":"center","flexWrap":"wrap"})
    ], style={"flex":"1","paddingLeft":"8px"})
], style={"display":"flex","gap":"8px"})

# -------- Painel por classificação
class_panel = html.Div([
    html.H4("Consulta por classificação (UR mínima no dia)"),
    html.Div([
        html.Div([
            html.Label("Classificação"),
            dcc.Dropdown(
                id="class-filter",
                options=[{"label": c, "value": c} for c in CLASS_ORDER],
                value="Emergência (<12%)",
                clearable=False,
            ),
        ], style={"minWidth":"320px","maxWidth":"380px","marginRight":"12px"}),
        html.Div(id="class-count", style={"alignSelf":"center","fontWeight":"800","fontSize":"16px"})
    ], style={"display":"flex","alignItems":"center","gap":"12px","flexWrap":"wrap","marginBottom":"8px"}),
    html.Div(id="class-list", style={
        "maxHeight":"28vh","overflowY":"auto","backgroundColor":"#f8fafc",
        "padding":"10px","borderRadius":"8px","border":"1px solid #e5e7eb"
    })
], style={"marginTop":"14px"})

app.layout = html.Div([
    html.H3(APP_TITLE),
    controls, two_cols, class_panel, store_sel
], style={"fontFamily":"Inter, system-ui, Arial", "padding":"12px"})

# ================== CALLBACKS ==================
@app.callback(
    Output("muni-filter", "options"),
    Output("muni-filter", "value"),
    Input("uf-filter", "value"),
    prevent_initial_call=False
)
def update_muni_dropdown(ufs_sel):
    df = get_data()
    if ufs_sel:
        df = df[df["SIGLA_UF"].isin(ufs_sel)]
    muni = df[["CD_MUN","NM_MUN","SIGLA_UF"]].drop_duplicates().sort_values(["SIGLA_UF","NM_MUN"])
    options = [{"label": f"{r.NM_MUN} / {r.SIGLA_UF}", "value": r.CD_MUN} for r in muni.itertuples()]
    return options, None

@app.callback(
    Output("muni-sel","data"),
    Input("mapa","clickData"),
    Input("muni-filter","value"),
    State("muni-sel","data")
)
def set_selection(clickData, dropdown_val, sel):
    if dropdown_val:
        return str(dropdown_val).zfill(7)
    if clickData and clickData.get("points"):
        cd = str(clickData["points"][0].get("location","")).zfill(7)
        if cd: return cd
    return sel

@app.callback(
    Output("mapa","figure"),
    Input("uf-filter","value"),
    Input("muni-filter","value"),
    Input("date-slider","value"),
)
def update_map(ufs_sel, muni_sel, date_idx):
    dff = get_data()
    if ufs_sel:
        dff = dff[dff["SIGLA_UF"].isin(ufs_sel)]
    if muni_sel:
        dff = dff[dff["CD_MUN"] == muni_sel]
    ds = current_dates()
    if ds:
        d_sel = ds[int(date_idx)]
        dff = dff[dff["data"] == d_sel]

    dff["Classe_UR"] = pd.Categorical(dff["RHmin"].apply(classificar_rhmin),
                                      categories=CLASS_ORDER, ordered=True)

    if gj is not None:
        fig = px.choropleth_mapbox(
            dff, geojson=gj, locations="CD_MUN", featureidkey="properties.CD_MUN",
            color="Classe_UR", color_discrete_map=COLOR_MAP,
            category_orders={"Classe_UR": CLASS_ORDER},
            hover_data={"NM_MUN": True, "SIGLA_UF": True, "RHmin":":.0f"},
            center={"lat": -14.2, "lon": -51.9}, zoom=3.5
        )
        fig.update_traces(marker_line_width=0)  # sem bordas
        fig.update_layout(
            mapbox_style="carto-positron",
            margin=dict(l=0,r=0,t=0,b=0),
            legend_title_text="Classificação (UR mínima)",
            legend=dict(x=1.02, y=1, traceorder="normal")
        )
    else:
        fig = px.scatter_mapbox(
            dff, lat="lat", lon="lon",
            color="Classe_UR", color_discrete_map=COLOR_MAP,
            hover_name="NM_MUN",
            hover_data={"SIGLA_UF": True, "RHmin":":.0f"},
            zoom=3.8, height=680
        )
        fig.update_traces(marker_line_width=0)
        fig.update_layout(mapbox_style="carto-positron", margin=dict(l=0,r=0,t=0,b=0),
                          legend_title_text="Classificação (UR mínima)")
    return fig

@app.callback(
    Output("grafico","figure"),
    Output("cards-ur","children"),
    Input("muni-sel","data")
)
def update_chart(sel_cd):
    dfm = get_data()
    dfm = dfm[dfm["CD_MUN"] == str(sel_cd).zfill(7)].sort_values("data").tail(5).copy()
    if dfm.empty:
        return go.Figure(), []

    dfm["Classe"] = dfm["RHmin"].apply(classificar_rhmin)
    dfm["Cor"]    = dfm["Classe"].map(COLOR_MAP)
    dfm["Dia"]    = pd.to_datetime(dfm["data"]).strftime("%d/%m")

    fig_bar = go.Figure(go.Bar(
        x=dfm["Dia"], y=dfm["RHmin"],
        marker_color=dfm["Cor"],
        text=dfm["RHmin"].round(0).astype(int).astype(str) + "%",
        textposition="outside", cliponaxis=False
    ))
    fig_bar.update_layout(
        title=f"UR mínima – {dfm['NM_MUN'].iloc[0]} / {dfm['SIGLA_UF'].iloc[0]}",
        yaxis_title="UR (%)", xaxis_title="", showlegend=False,
        margin=dict(l=10, r=10, t=50, b=40)
    )
    fig_bar.update_yaxes(range=[0, max(40, float(dfm["RHmin"].max() or 0) + 5)], ticks="outside")

    cards=[]
    for _, r in dfm.iterrows():
        cls = r["Classe"]; bg = COLOR_MAP.get(cls, "#9CA3AF")
        cards.append(
            html.Div([
                html.Div(r["Dia"], style={"fontSize":"14px","fontWeight":"800","marginBottom":"4px"}),
                html.Div(f"{r['RHmin']:.0f}%", style={"fontSize":"20px","fontWeight":"900","marginBottom":"4px"}),
                html.Div(str(cls), style={"fontSize":"14px","fontWeight":"800"})
            ], style={
                "backgroundColor": bg,
                "color": "#0b0b0b" if cls in ["Quase ideal (41–60%)","Observação (30–40%)"] else "white",
                "borderRadius":"12px","padding":"10px 12px","textAlign":"center",
                "minWidth":"90px","boxShadow":"0 1px 2px rgba(0,0,0,.08)"
            })
        )
    return fig_bar, cards

@app.callback(
    Output("class-count","children"),
    Output("class-list","children"),
    Input("class-filter","value"),
    Input("uf-filter","value"),
    Input("muni-filter","value"),
    Input("date-slider","value"),
)
def list_by_class(sel_class, ufs_sel, muni_sel, date_idx):
    dff = get_data()
    if ufs_sel:
        dff = dff[dff["SIGLA_UF"].isin(ufs_sel)]
    if muni_sel:
        dff = dff[dff["CD_MUN"] == muni_sel]
    ds = current_dates()
    if ds:
        d_sel = ds[int(date_idx)]
        dff = dff[dff["data"] == d_sel]

    dff["Classe_UR"] = dff["RHmin"].apply(classificar_rhmin)
    if sel_class:
        dff = dff[dff["Classe_UR"] == sel_class]

    if dff.empty:
        return html.Div("0 município(s) na categoria selecionada."), html.Div("Nenhum município com os filtros atuais.")

    dff = dff.dropna(subset=["RHmin"])
    dff = dff[["CD_MUN","NM_MUN","SIGLA_UF","RHmin"]].drop_duplicates("CD_MUN").sort_values(["SIGLA_UF","NM_MUN"])

    count = len(dff)
    today = datetime.now(BRT).strftime("%d/%m")
    header = html.Div(f"{count} município(s) na categoria: {sel_class} – {today}")

    lines = "\n".join(f"- {row.NM_MUN} / {row.SIGLA_UF} — {int(round(row.RHmin))}%"
                      for row in dff.itertuples())
    return header, dcc.Markdown(lines)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8060)), debug=False)



