from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

st.set_page_config(page_title="Dashboard PNAD COVID", layout="wide")

# =========================================================
# CONFIGURAÇÃO DOS ARQUIVOS
# =========================================================
# O dashboard procura os arquivos nestas pastas:
# 1) caminho informado abaixo
# 2) pasta atual do app
# 3) output/specs
# 4) ../output/specs
# Assim fica mais fácil rodar tanto local quanto dentro do projeto.

BASE_SPECS_PATH = Path(r"../output/specs")

ARQUIVOS = {
    # Specs originais
    "Sintomas Clínicos": "spec_sintomas_clinicos.csv",
    "Comportamento": "spec_comportamento.csv",
    "Econômico": "spec_economico.csv",

    # Novos specs adicionados
    "01 - Evolução geral dos sintomas gripais": "01_evolucao_geral_sintomas_gripais.csv",
    "02 - Sintomas gripais por região": "02_sintomas_gripais_por_regiao.csv",
    "03 - Sintomas por faixa etária": "03_sintomas_por_faixa_etaria.csv",
    "04 - Gravidade: dificuldade para respirar": "04_gravidade_dificuldade_respirar.csv",
    "05 - Sintomas com comorbidades": "05_sintomas_com_comorbidades.csv",
    "06 - Busca por atendimento de saúde": "06_busca_atendimento_saude.csv",
    "09 - Home office por região": "09_home_office_por_regiao.csv",
    "10 - Situação do mercado de trabalho": "10_situacao_mercado_trabalho.csv",
    "11 - Dependência de auxílio emergencial": "11_dependencia_auxilio_emergencial.csv",
    "12 - Renda efetiva média por região": "12_renda_efetiva_media_por_regiao.csv",
    "13 - Insights para prevenção de novo surto": "13_insights_prevencao_proximo_surto.csv",
}

PASTAS_BUSCA = [
    BASE_SPECS_PATH,
    Path("."),
    Path("output/specs"),
    Path("../output/specs"),
    Path("/mnt/data"),  # útil apenas quando testado no ambiente do ChatGPT
]


def localizar_arquivo(nome_arquivo: str) -> Path | None:
    """Procura o CSV em múltiplas pastas para evitar erro de caminho."""
    caminho = Path(nome_arquivo)

    if caminho.exists():
        return caminho

    for pasta in PASTAS_BUSCA:
        candidato = pasta / nome_arquivo
        if candidato.exists():
            return candidato

    return None


@st.cache_data
def carregar_csv(nome_arquivo: str) -> pd.DataFrame:
    caminho = localizar_arquivo(nome_arquivo)

    if caminho is None:
        raise FileNotFoundError(
            f"Arquivo não encontrado: {nome_arquivo}. "
            "Verifique se ele está em output/specs, ../output/specs ou na mesma pasta do dashboard."
        )

    # sep=None + engine='python' detecta automaticamente CSV com vírgula ou ponto e vírgula.
    df = pd.read_csv(caminho, sep=None, engine="python")

    # Padroniza nomes de colunas sem quebrar os CSVs antigos.
    df.columns = [str(c).strip() for c in df.columns]

    renomear = {
        "MES_REF": "mes_ref",
        "MÊS_REF": "mes_ref",
        "Mes_Ref": "mes_ref",
    }
    df = df.rename(columns={c: renomear.get(c, c) for c in df.columns})

    # Conversão automática de colunas numéricas.
    for col in df.columns:
        if col == "mes_ref":
            continue
        convertido = pd.to_numeric(df[col], errors="coerce")
        if convertido.notna().sum() > 0 and convertido.notna().sum() >= df[col].notna().sum() * 0.6:
            df[col] = convertido.round(2)

    return df


def colunas_numericas(df: pd.DataFrame) -> list[str]:
    return df.select_dtypes(include="number").columns.tolist()


def colunas_categoricas(df: pd.DataFrame) -> list[str]:
    return df.select_dtypes(exclude="number").columns.tolist()


def aplicar_filtros(df: pd.DataFrame, nome: str) -> pd.DataFrame:
    df_filtrado = df.copy()

    st.sidebar.header(f"Filtros - {nome}")

    colunas_filtro = [
        "grupo",
        "mes_ref",
        "regiao",
        "faixa_etaria",
        "situacao_mercado_trabalho",
        "descricao",
    ]

    for col in colunas_filtro:
        if col in df_filtrado.columns:
            valores = sorted(df_filtrado[col].dropna().astype(str).unique().tolist())

            if not valores:
                continue

            selecionados = st.sidebar.multiselect(
                f"{col} - {nome}",
                options=valores,
                default=valores,
                key=f"filtro_{nome}_{col}",
            )

            df_filtrado = df_filtrado[df_filtrado[col].astype(str).isin(selecionados)]

    return df_filtrado


def exibir_kpis(df: pd.DataFrame, nums: list[str]) -> None:
    if not nums:
        return

    st.subheader("📌 Indicadores principais")

    colunas = st.columns(min(3, len(nums)))

    for i, col in enumerate(nums[:3]):
        valor = df[col].mean()
        if pd.notna(valor):
            colunas[i % len(colunas)].metric(f"Média - {col}", f"{valor:,.2f}")


def grafico_legado(df: pd.DataFrame, nome: str) -> bool:
    """Mantém o comportamento dos specs antigos: pct_ponderado, valor_medio_ponderado, grupo e descricao."""
    if "pct_ponderado" not in df.columns and "valor_medio_ponderado" not in df.columns:
        return False

    df_mensal = df.copy()
    if "mes_ref" in df_mensal.columns:
        df_mensal = df_mensal[df_mensal["mes_ref"].astype(str) != "TOTAL"]

    df_total = pd.DataFrame()
    if "mes_ref" in df.columns:
        df_total = df[df["mes_ref"].astype(str) == "TOTAL"]

    if not df_mensal.empty and "pct_ponderado" in df_mensal.columns and "mes_ref" in df_mensal.columns:
        st.subheader("📈 Evolução mensal por indicador")
        fig = px.line(
            df_mensal,
            x="mes_ref",
            y="pct_ponderado",
            color="descricao" if "descricao" in df_mensal.columns else None,
            markers=True,
            text="pct_ponderado",
            title=f"Evolução mensal - {nome}",
        )
        fig.update_traces(textposition="top center")
        fig.update_layout(xaxis_title="Mês", yaxis_title="Percentual ponderado (%)")
        st.plotly_chart(fig, width="stretch")

        st.subheader("📊 Comparação mensal")
        fig = px.bar(
            df_mensal,
            x="mes_ref",
            y="pct_ponderado",
            color="descricao" if "descricao" in df_mensal.columns else None,
            barmode="group",
            text="pct_ponderado",
            title=f"Comparação mensal - {nome}",
        )
        fig.update_traces(texttemplate="%{text:.2f}%", textposition="outside")
        fig.update_layout(xaxis_title="Mês", yaxis_title="Percentual ponderado (%)")
        st.plotly_chart(fig, width="stretch")

    if not df_total.empty and "pct_ponderado" in df_total.columns:
        st.subheader("🏁 Consolidado TOTAL")
        fig = px.bar(
            df_total.sort_values("pct_ponderado", ascending=True),
            x="pct_ponderado",
            y="descricao" if "descricao" in df_total.columns else None,
            color="grupo" if "grupo" in df_total.columns else None,
            orientation="h",
            text="pct_ponderado",
            title=f"Indicadores consolidados - {nome}",
        )
        fig.update_traces(texttemplate="%{text:.2f}%", textposition="outside")
        fig.update_layout(xaxis_title="Percentual ponderado (%)", yaxis_title="Indicador")
        st.plotly_chart(fig, width="stretch")

    if "valor_medio_ponderado" in df.columns:
        df_valor = df[df["valor_medio_ponderado"].notna()]
        if not df_valor.empty:
            st.subheader("💰 Valor médio ponderado")
            fig = px.bar(
                df_valor.sort_values("valor_medio_ponderado", ascending=True),
                x="valor_medio_ponderado",
                y="descricao" if "descricao" in df_valor.columns else None,
                color="grupo" if "grupo" in df_valor.columns else None,
                orientation="h",
                text="valor_medio_ponderado",
                title=f"Valores médios ponderados - {nome}",
            )
            fig.update_traces(texttemplate="R$ %{text:.2f}", textposition="outside")
            fig.update_layout(xaxis_title="Valor médio ponderado (R$)", yaxis_title="Indicador")
            st.plotly_chart(fig, width="stretch")

    return True


def grafico_generico(df: pd.DataFrame, nome: str) -> None:
    nums = colunas_numericas(df)
    cats = colunas_categoricas(df)

    if not nums:
        st.info("Este arquivo não possui colunas numéricas para gráfico.")
        return

    metrica = st.selectbox(
        "Selecione a métrica para visualizar",
        options=nums,
        key=f"metrica_{nome}",
    )

    if "mes_ref" in df.columns:
        st.subheader("📈 Evolução mensal")

        # Se tiver região/faixa/situação/descrição, usa como série.
        categoria_serie = None
        for candidato in ["regiao", "faixa_etaria", "situacao_mercado_trabalho", "descricao", "grupo"]:
            if candidato in df.columns:
                categoria_serie = candidato
                break

        if categoria_serie:
            fig = px.line(
                df.sort_values("mes_ref"),
                x="mes_ref",
                y=metrica,
                color=categoria_serie,
                markers=True,
                text=metrica,
                title=f"{metrica} por mês - {nome}",
            )
        else:
            fig = px.line(
                df.sort_values("mes_ref"),
                x="mes_ref",
                y=metrica,
                markers=True,
                text=metrica,
                title=f"{metrica} por mês - {nome}",
            )

        fig.update_traces(textposition="top center")
        fig.update_layout(xaxis_title="Mês", yaxis_title=metrica)
        st.plotly_chart(fig, width="stretch")

    categoria_barra = None
    for candidato in ["regiao", "faixa_etaria", "situacao_mercado_trabalho", "descricao", "grupo"]:
        if candidato in df.columns:
            categoria_barra = candidato
            break

    if categoria_barra:
        st.subheader("📊 Comparação por categoria")
        fig = px.bar(
            df.sort_values(metrica, ascending=False),
            x=categoria_barra,
            y=metrica,
            color="mes_ref" if "mes_ref" in df.columns else categoria_barra,
            barmode="group",
            text=metrica,
            title=f"{metrica} por {categoria_barra} - {nome}",
        )
        fig.update_traces(texttemplate="%{text:.2f}", textposition="outside")
        fig.update_layout(xaxis_title=categoria_barra, yaxis_title=metrica)
        st.plotly_chart(fig, width="stretch")

    # Arquivos com várias métricas por mês/região, ex.: insights de prevenção.
    if len(nums) > 1 and "mes_ref" in df.columns:
        st.subheader("🧭 Comparação de múltiplos indicadores")
        id_vars = [c for c in ["mes_ref", "regiao", "faixa_etaria", "situacao_mercado_trabalho"] if c in df.columns]
        df_long = df.melt(id_vars=id_vars, value_vars=nums, var_name="indicador", value_name="valor")

        fig = px.line(
            df_long.sort_values("mes_ref"),
            x="mes_ref",
            y="valor",
            color="indicador",
            line_dash="regiao" if "regiao" in df_long.columns else None,
            markers=True,
            title=f"Indicadores comparados - {nome}",
        )
        fig.update_layout(xaxis_title="Mês", yaxis_title="Valor")
        st.plotly_chart(fig, width="stretch")


# =========================================================
# APP STREAMLIT
# =========================================================

st.title("📊 Dashboard PNAD COVID")
st.markdown(
    "Dashboard consolidado com os specs originais e os novos indicadores de saúde, "
    "comportamento, trabalho, renda e prevenção para um possível novo surto."
)

with st.expander("📁 Arquivos esperados pelo dashboard"):
    st.write(pd.DataFrame({"Aba": list(ARQUIVOS.keys()), "Arquivo CSV": list(ARQUIVOS.values())}))

abas = st.tabs(list(ARQUIVOS.keys()))

for aba, (nome, arquivo) in zip(abas, ARQUIVOS.items()):
    with aba:
        st.header(f"📌 {nome}")

        try:
            df = carregar_csv(arquivo)
        except Exception as erro:
            st.error(str(erro))
            continue

        df_filtrado = aplicar_filtros(df, nome)
        nums = colunas_numericas(df_filtrado)

        exibir_kpis(df_filtrado, nums)
        st.divider()

        # Primeiro tenta o layout original dos specs antigos.
        usou_layout_legado = grafico_legado(df_filtrado, nome)

        # Depois usa o layout genérico para os CSVs novos.
        if not usou_layout_legado:
            grafico_generico(df_filtrado, nome)

        st.subheader("📄 Dados filtrados")
        st.dataframe(df_filtrado, width="stretch")

        csv_download = df_filtrado.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
        st.download_button(
            label="⬇️ Baixar dados filtrados em CSV",
            data=csv_download,
            file_name=f"{nome.lower().replace(' ', '_').replace('-', '').replace(':', '')}_filtrado.csv",
            mime="text/csv",
            key=f"download_{nome}",
        )
