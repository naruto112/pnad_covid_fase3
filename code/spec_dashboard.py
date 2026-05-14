import streamlit as st
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="Dashboard PNAD COVID", layout="wide")

ARQUIVOS = {
    "Sintomas Clínicos": r"..\output\specs\spec_sintomas_clinicos.csv",
    "Comportamento": r"..\output\specs\spec_comportamento.csv",
    "Econômico": r"..\output\specs\spec_economico.csv",
}

@st.cache_data
def carregar_csv(caminho):
    df = pd.read_csv(caminho)

    if "pct_ponderado" in df.columns:
        df["pct_ponderado"] = pd.to_numeric(df["pct_ponderado"], errors="coerce").round(2)

    if "valor_medio_ponderado" in df.columns:
        df["valor_medio_ponderado"] = pd.to_numeric(df["valor_medio_ponderado"], errors="coerce").round(2)

    return df


st.title("📊 Dashboard PNAD COVID")
st.markdown("Análise dos arquivos `spec_sintomas_clinicos`, `spec_comportamento` e `spec_economico`.")

abas = st.tabs(list(ARQUIVOS.keys()))

for aba, (nome, caminho) in zip(abas, ARQUIVOS.items()):

    with aba:
        st.header(f"📌 {nome}")

        df = carregar_csv(caminho)

        colunas = df.columns.tolist()

        st.sidebar.header(f"Filtros - {nome}")

        grupos = st.sidebar.multiselect(
            f"Grupo - {nome}",
            options=sorted(df["grupo"].dropna().unique()),
            default=sorted(df["grupo"].dropna().unique())
        )

        df_filtrado = df[df["grupo"].isin(grupos)]

        if "mes_ref" in df_filtrado.columns:
            meses = st.sidebar.multiselect(
                f"Mês - {nome}",
                options=sorted(df_filtrado["mes_ref"].dropna().unique()),
                default=sorted(df_filtrado["mes_ref"].dropna().unique())
            )

            df_filtrado = df_filtrado[df_filtrado["mes_ref"].isin(meses)]

        df_mensal = df_filtrado[df_filtrado["mes_ref"] != "TOTAL"]
        df_total = df_filtrado[df_filtrado["mes_ref"] == "TOTAL"]

        # =====================
        # KPIs
        # =====================
        col1, col2, col3 = st.columns(3)

        if "pct_ponderado" in df_filtrado.columns:
            col1.metric("Maior percentual", f"{df_filtrado['pct_ponderado'].max():.2f}%")
            col2.metric("Menor percentual", f"{df_filtrado['pct_ponderado'].min():.2f}%")
            col3.metric("Média percentual", f"{df_filtrado['pct_ponderado'].mean():.2f}%")

        st.divider()

        # =====================
        # GRÁFICO LINHA %
        # =====================
        if not df_mensal.empty and "pct_ponderado" in colunas:

            st.subheader("📈 Evolução mensal por indicador")

            fig_linha = px.line(
                df_mensal,
                x="mes_ref",
                y="pct_ponderado",
                color="descricao",
                markers=True,
                text="pct_ponderado",
                title=f"Evolução mensal - {nome}"
            )

            fig_linha.update_traces(textposition="top center")
            fig_linha.update_layout(
                xaxis_title="Mês",
                yaxis_title="Percentual ponderado (%)",
                legend_title="Indicador"
            )

            st.plotly_chart(fig_linha, width="stretch")

        # =====================
        # BARRAS MENSAIS %
        # =====================
        if not df_mensal.empty and "pct_ponderado" in colunas:

            st.subheader("📊 Comparação mensal")

            fig_barras = px.bar(
                df_mensal,
                x="mes_ref",
                y="pct_ponderado",
                color="descricao",
                barmode="group",
                text="pct_ponderado",
                title=f"Comparação mensal - {nome}"
            )

            fig_barras.update_traces(
                texttemplate="%{text:.2f}%",
                textposition="outside"
            )

            fig_barras.update_layout(
                xaxis_title="Mês",
                yaxis_title="Percentual ponderado (%)",
                legend_title="Indicador"
            )

            st.plotly_chart(fig_barras, width="stretch")

        # =====================
        # TOTAL %
        # =====================
        if not df_total.empty and "pct_ponderado" in colunas:

            st.subheader("🏁 Consolidado TOTAL")

            fig_total = px.bar(
                df_total.sort_values("pct_ponderado", ascending=True),
                x="pct_ponderado",
                y="descricao",
                color="grupo",
                orientation="h",
                text="pct_ponderado",
                title=f"Indicadores consolidados - {nome}"
            )

            fig_total.update_traces(
                texttemplate="%{text:.2f}%",
                textposition="outside"
            )

            fig_total.update_layout(
                xaxis_title="Percentual ponderado (%)",
                yaxis_title="Indicador",
                legend_title="Grupo"
            )

            st.plotly_chart(fig_total, width="stretch")

        # =====================
        # VALOR MÉDIO - ECONÔMICO
        # =====================
        if "valor_medio_ponderado" in colunas:

            df_valor = df_filtrado[df_filtrado["valor_medio_ponderado"].notna()]

            if not df_valor.empty:

                st.subheader("💰 Valor médio ponderado")

                fig_valor = px.bar(
                    df_valor.sort_values("valor_medio_ponderado", ascending=True),
                    x="valor_medio_ponderado",
                    y="descricao",
                    color="grupo",
                    orientation="h",
                    text="valor_medio_ponderado",
                    title=f"Valores médios ponderados - {nome}"
                )

                fig_valor.update_traces(
                    texttemplate="R$ %{text:.2f}",
                    textposition="outside"
                )

                fig_valor.update_layout(
                    xaxis_title="Valor médio ponderado (R$)",
                    yaxis_title="Indicador",
                    legend_title="Grupo"
                )

                st.plotly_chart(fig_valor, width="stretch")

        # =====================
        # PIZZA PARA TOTAL
        # =====================
        if not df_total.empty and "pct_ponderado" in colunas:

            st.subheader("🍩 Distribuição percentual TOTAL")

            fig_pizza = px.pie(
                df_total,
                names="descricao",
                values="pct_ponderado",
                hole=0.45,
                title=f"Distribuição TOTAL - {nome}"
            )

            st.plotly_chart(fig_pizza, width="stretch")

        # =====================
        # TABELA
        # =====================
        st.subheader("📄 Dados filtrados")
        st.dataframe(df_filtrado, width="stretch")