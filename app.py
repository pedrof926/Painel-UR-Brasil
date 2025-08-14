# app.py
# Painel de Umidade Relativa – Brasil (UR mínima, 5 dias)
# ✔ Único arquivo: pipeline INMET + cache diário (BRT) + Dash
# ✔ Sem XLSX intermediário (apenas lê o XLSX de municípios)

import os, json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dash import Dash, dcc, html, Input, Output, State
import plotly.express as px
import plotly.graph_objects as go
import pytz
from flask import request, jsonify

# ================== CONFIG ==================
APP_TITLE   = "Umidade Relativa do Ar – Brasil (UR mínima, 5 dias)"
BRT         = pytz.timezone("America/Sao_Paulo")

# Caminhos dentro do repo
ATTR_XLSX    = os.environ.get("ATTR_XLSX", "data/arquivo_completo_brasil.xlsx")  # municípios (CD_MUN,NM_MUN,SIGLA_UF,lat,lon)
GEOJSON_PATH = os.environ.get("GEOJSON_PATH", "geo/municipios_br.geojson")       # opcional; se ausente, cai p/ pontos
REFRESH_TOKEN= os.environ.get("REFRESH_TOKEN", "")                                # opcional para /refresh?token=...

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

# ================== GEOJSON (opcional) ==================
def load_geojson(path):
    if not path or not os.path.exists(path): return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            gj = json.load(f)
        for ft in gj.get("features", []):
            props = ft.setdefault("properties", {})
            props["CD_MUN"] = str(props.get("CD_MUN","")).zfill(7)
        return gj
    except Exception as e:
        print("[geo] Falha ao ler GeoJSON:", e)
        return None

gj = load_geojson(GEOJSON_PATH)

# ================== PIPELINE INMET (cole aqui o seu ETL) ==================
def z7(s):  # zfill(7) robusto
    return pd.Series([s], dtype=str).str.extract(r"(\d+)")[0].iloc[0].zfill(7)

def load_attr_municipios(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise SystemExit(f"❌ Não encontrei o arquivo de municípios: {path}")
    attr = pd.read_excel(path, engine="openpyxl")
    # tenta mapear nomes de coluna comuns
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

def build_df(attr_xlsx_path: str) -> pd.DataFrame:
    """
    >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    TODO INMET: COLE AQUI seu pipeline que hoje gera a previsão (5 dias) e RETORNE um
    DataFrame com colunas:
      CD_MUN (str, 7 dígitos), NM_MUN, SIGLA_UF, lat, lon, data (date), RHmin (float), RHmax (opcional)
    Dica: onde você fazia df.to_excel(...), troque por: return df
    >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    """
    # Exemplo de estrutura esperada (REMOVA após colar seu pipeline):
    # base = load_attr_municipios(attr_xlsx_path)
    # df_final = seu_etl_inmet(base)  # deve devolver colunas acima
    # return df_final
    raise NotImplementedError("Cole o seu pipeline do INMET dentro de build_df(...).")

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
    # dados de demonstração (3 capitais × 5 dias) para validar a UI
    base = pd.DataFrame({
        "CD_MUN": ["5300108","3550308","3304557"],
        "NM_MUN": ["Brasília","São Paulo","Rio de Janeiro"],
        "SIGLA_UF": ["DF","SP","RJ"],
        "lat": [-15.78,-23.55,-22.90],
        "lon": [-47.93,-46.63,-43.17],
    })
    today = datetime.now(BRT).date()
    rows=[]
    for _, r in base.iterrows():
        for i in range(5):
            d = today + timedelta(days=i)
            rhmin = [55, 35, 18, 62, 28][i % 5]  # valores só p/ demonstrar as classes
            rows.append({**r.to_dict(), "data": d, "RHmin": rhmin, "RHmax": np.nan})
    return pd.DataFrame(rows)

def get_data(force=False) -> pd.DataFrame:
    key = datetime.now(BRT).strftime("%Y-%m-%d")  # muda a cada virada de dia no Brasil
    if (not force) and _CACHE["key"] == key and _CACHE["df"] is not None:
        return _CACHE["df"]
    print(f"[umidade] (re)montando dados – chave diária {key}")
    try:
        df = build_df(ATTR_XLSX)
    except NotImplementedError as e:
        print(">>> AVISO:", e)
        df = _demo_df()
    df = _sanitize_df(df)
    _CACHE.update(key=key, df=df)
    return df

# ================== APP (Dash) ==================
app = Dash(__name__)
server = app.server
app.title = APP_TITLE

# endpoint opcional de refresh manual
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

# dados iniciais
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
# Municípios dependentes do(s) UF(s)
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

# Seleção global (clique no mapa OU dropdown)
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

# Mapa por classe (UR mínima no dia)
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
        fig.update_traces(marker_line_width=0)  # sem linhas de município
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

# Gráfico 5 dias + cards (pela seleção global)
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

# Lista por classificação (no dia)
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

