"""
Dashboard — Análise Refinada PNAD COVID-19
==========================================
Reproduz, de forma interativa, o estudo do notebook `refined_analysis_pnad.ipynb`:
introdução, três pilares analíticos (Sintomas Clínicos, Comportamento, Econômico)
e a conclusão com 5 eixos de recomendações hospitalares.

Os dados são lidos diretamente das tabelas Parquet em `output/pnad_covid` via DuckDB
(sem depender de Spark). Todas as agregações são ponderadas por `peso_amostral`,
como no notebook original.
"""

from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

st.set_page_config(page_title="Análise Refinada PNAD COVID", layout="wide")

# =========================================================
# CONSTANTES (replicadas do refined_analysis_pnad.ipynb)
# =========================================================
MESES = ["2020-09", "2020-10", "2020-11"]
MES_LABEL = {"2020-09": "Set/2020", "2020-10": "Out/2020", "2020-11": "Nov/2020"}

TABELAS = ["base_saude", "base_comportamento", "base_economico", "dim_perfil", "dim_localizacao"]
JOIN_KEYS = ["uf", "id_domicilio", "id_morador", "mes_entrevista", "mes_ref"]

ORDEM_FAIXA = ["00-17", "18-29", "30-44", "45-59", "60-74", "75+"]

# 4 níveis de restrição de contato + rótulos curtos
ORDEM_RESTR = [
    "Não fez restrição, levou vida normal como antes da pandemia",
    "Reduziu o contato com as pessoas, mas continuou saindo de casa para trabalho ou "
    "atividades não essenciais e/ou recebendo visitas",
    "Ficou em casa e só saiu em caso de necessidade básica",
    "Ficou rigorosamente em casa",
]
LABEL_RESTR = {
    ORDEM_RESTR[0]: "Sem restrição",
    ORDEM_RESTR[1]: "Reduziu contato",
    ORDEM_RESTR[2]: "Só saiu p/ necessidades",
    ORDEM_RESTR[3]: "Rigorosamente em casa",
}

SINTOMAS = {
    "sintoma_febre": "Febre",
    "sintoma_tosse": "Tosse",
    "sintoma_dificuldade_respirar": "Dificuldade de respirar",
    "sintoma_perda_olfato_paladar": "Perda de olfato/paladar",
    "sintoma_fadiga": "Fadiga",
}
COMORBIDADES = {
    "medico_deu_diagnostico_diabetes": "Diabetes",
    "medico_deu_diagnostico_hipertensao": "Hipertensão",
    "medico_deu_diagnostico_doencas_coracao_infarto_angina": "Doença cardíaca",
}
LOCAIS_ATENDIMENTO = {
    "local_buscou_atendimento_posto_saude_unidade_basica_saude": "Posto de saúde / UBS",
    "local_buscou_atendimento_pronto_socorro_sus_upa": "Pronto-socorro SUS / UPA",
    "local_buscou_atendimento_hospital_sus": "Hospital SUS",
    "local_buscou_atendimento_ambulatorio_consultorio_privado": "Ambulatório/consultório privado",
    "local_buscou_atendimento_pronto_socorro_privado_ligado": "Pronto-socorro privado",
    "local_buscou_atendimento_hospital_privado_ligado_forcas": "Hospital privado",
}
BENEFICIOS = {
    "recebeu_auxilio_emergencial": "Auxílio Emergencial",
    "recebeu_aposentadoria_pensao": "Aposentadoria / Pensão",
    "recebeu_bolsa_familia": "Bolsa Família",
    "recebeu_seguro_desemprego": "Seguro Desemprego",
}
SALARIO_MINIMO_2020 = 1045.0

# =========================================================
# CAMADA DE DADOS — DuckDB sobre Parquet
# =========================================================
APP_DIR = Path(__file__).resolve().parent


def localizar_base() -> Path:
    """Procura a pasta output/pnad_covid em locais prováveis."""
    candidatos = [
        APP_DIR.parent / "output" / "pnad_covid",
        APP_DIR / "output" / "pnad_covid",
        Path("output/pnad_covid"),
        Path("../output/pnad_covid"),
    ]
    for c in candidatos:
        if (c / "base_saude").exists():
            return c.resolve()
    raise FileNotFoundError(
        "Não encontrei a pasta output/pnad_covid com as tabelas Parquet. "
        "Rode o dashboard a partir da pasta 'code' do projeto."
    )


@st.cache_resource(show_spinner="Conectando às tabelas Parquet...")
def get_con() -> duckdb.DuckDBPyConnection:
    base = localizar_base()
    con = duckdb.connect(database=":memory:")
    for t in TABELAS:
        padrao = str(base / t / "**" / "*.parquet").replace("\\", "/")
        con.execute(
            f"CREATE OR REPLACE VIEW {t} AS "
            f"SELECT * FROM read_parquet('{padrao}', hive_partitioning=true, union_by_name=true)"
        )
    return con


@st.cache_data(show_spinner=False)
def q(sql: str) -> pd.DataFrame:
    """Executa SQL no DuckDB e devolve um DataFrame (cacheado pela string da query)."""
    return get_con().execute(sql).df()


def join_cond(a: str, b: str) -> str:
    """Condição de join entre duas tabelas pela chave de domicílio/morador/mês."""
    return " AND ".join(f"{a}.{k} = {b}.{k}" for k in JOIN_KEYS)


def filtro_mes(meses, alias: str = "") -> str:
    """Cláusula WHERE para os meses selecionados."""
    col = f"{alias}.mes_ref" if alias else "mes_ref"
    lista = ", ".join(f"'{m}'" for m in meses)
    return f"{col} IN ({lista})"


# =========================================================
# CONSULTAS ANALÍTICAS (uma por análise do notebook)
# =========================================================
def prevalencia_sintomas(meses) -> pd.DataFrame:
    cols = ",\n".join(
        f"SUM(CASE WHEN {c} = 'Sim' THEN peso_amostral ELSE 0 END) "
        f"/ SUM(peso_amostral) * 100 AS \"{lab}\""
        for c, lab in SINTOMAS.items()
    )
    df = q(f"SELECT {cols} FROM base_saude WHERE {filtro_mes(meses)}")
    return df.T.reset_index().set_axis(["sintoma", "pct"], axis=1).sort_values("pct")


def evolucao_sintomas() -> pd.DataFrame:
    cols = ",\n".join(
        f"SUM(CASE WHEN {c} = 'Sim' THEN peso_amostral ELSE 0 END) "
        f"/ SUM(peso_amostral) * 100 AS \"{lab}\""
        for c, lab in SINTOMAS.items()
    )
    df = q(f"SELECT mes_ref AS mes_ref, {cols} FROM base_saude GROUP BY mes_ref ORDER BY mes_ref")
    longo = df.melt(id_vars="mes_ref", var_name="sintoma", value_name="pct")
    longo["mes"] = longo["mes_ref"].map(MES_LABEL)
    return longo


def qtd_sintomas(meses) -> pd.DataFrame:
    df = q(
        f"SELECT qtd_sintomas_relatados AS qtd, SUM(peso_amostral) AS pop "
        f"FROM base_saude WHERE {filtro_mes(meses)} "
        f"GROUP BY qtd_sintomas_relatados ORDER BY qtd_sintomas_relatados"
    )
    df["pct"] = df["pop"] / df["pop"].sum() * 100
    return df


def testagem(meses) -> pd.DataFrame:
    df = q(
        f"""
        SELECT
            SUM(CASE WHEN fez_teste_covid = 'Sim' THEN peso_amostral ELSE 0 END)
                / SUM(peso_amostral) * 100 AS pct_testou,
            SUM(CASE WHEN resultado_teste_positivo = 'Positivo' THEN peso_amostral ELSE 0 END)
                / SUM(peso_amostral) * 100 AS pct_pos_geral,
            SUM(CASE WHEN resultado_teste_positivo = 'Positivo' THEN peso_amostral ELSE 0 END)
                / NULLIF(SUM(CASE WHEN fez_teste_covid = 'Sim' THEN peso_amostral ELSE 0 END), 0)
                * 100 AS pct_pos_testados
        FROM base_saude WHERE {filtro_mes(meses)}
        """
    )
    return pd.DataFrame(
        {
            "indicador": [
                "Realizou teste de COVID",
                "Positivos na população geral",
                "Positivos entre os testados",
            ],
            "pct": [df.pct_testou[0], df.pct_pos_geral[0], df.pct_pos_testados[0]],
        }
    )


def comorbidades_geral_positivos(meses) -> pd.DataFrame:
    def linha(filtro_extra):
        cols = ",\n".join(
            f"SUM(CASE WHEN {c} = 'Sim' THEN peso_amostral ELSE 0 END) "
            f"/ SUM(peso_amostral) * 100 AS \"{lab}\""
            for c, lab in COMORBIDADES.items()
        )
        return q(f"SELECT {cols} FROM base_saude WHERE {filtro_mes(meses)} {filtro_extra}")

    geral = linha("").T.reset_index().set_axis(["comorbidade", "pct"], axis=1)
    geral["grupo"] = "População geral"
    pos = linha("AND resultado_teste_positivo = 'Positivo'")
    pos = pos.T.reset_index().set_axis(["comorbidade", "pct"], axis=1)
    pos["grupo"] = "Testados positivos"
    return pd.concat([geral, pos], ignore_index=True)


def comorbidades_por_faixa(meses) -> pd.DataFrame:
    cols = ",\n".join(
        f"SUM(CASE WHEN s.{c} = 'Sim' THEN s.peso_amostral ELSE 0 END) "
        f"/ SUM(s.peso_amostral) * 100 AS \"{lab}\""
        for c, lab in COMORBIDADES.items()
    )
    df = q(
        f"""
        SELECT p.faixa_etaria, {cols}
        FROM base_saude s
        JOIN dim_perfil p ON {join_cond('s', 'p')}
        WHERE {filtro_mes(meses, 's')}
        GROUP BY p.faixa_etaria
        """
    )
    longo = df.melt(id_vars="faixa_etaria", var_name="comorbidade", value_name="pct")
    return longo


def taxa_sintoma_por(meses, coluna: str) -> pd.DataFrame:
    return q(
        f"""
        SELECT p.{coluna} AS categoria,
               SUM(s.ind_teve_sintoma_gripal * s.peso_amostral)
                   / SUM(s.peso_amostral) * 100 AS pct
        FROM base_saude s
        JOIN dim_perfil p ON {join_cond('s', 'p')}
        WHERE {filtro_mes(meses, 's')}
        GROUP BY p.{coluna}
        """
    )


def taxa_sintoma_faixa_sexo(meses) -> pd.DataFrame:
    return q(
        f"""
        SELECT p.faixa_etaria, p.sexo,
               SUM(s.ind_teve_sintoma_gripal * s.peso_amostral)
                   / SUM(s.peso_amostral) * 100 AS pct
        FROM base_saude s
        JOIN dim_perfil p ON {join_cond('s', 'p')}
        WHERE {filtro_mes(meses, 's')}
        GROUP BY p.faixa_etaria, p.sexo
        """
    )


def isolamento_x_trabalho(meses) -> pd.DataFrame:
    df = q(
        f"""
        SELECT c.situacao_mercado_trabalho AS situacao,
               s.restricao_contato_pessoas_pandemia AS restricao,
               SUM(s.peso_amostral) AS pop
        FROM base_saude s
        JOIN base_comportamento c ON {join_cond('s', 'c')}
        WHERE {filtro_mes(meses, 's')}
          AND s.restricao_contato_pessoas_pandemia IN ({", ".join(repr(r) for r in ORDEM_RESTR)})
          AND c.situacao_mercado_trabalho <> 'Nao se aplica'
        GROUP BY situacao, restricao
        """
    )
    df["restricao"] = df["restricao"].map(LABEL_RESTR)
    df["pct"] = df["pop"] / df.groupby("situacao")["pop"].transform("sum") * 100
    return df


def evolucao_comportamento() -> pd.DataFrame:
    linhas = []
    for m in MESES:
        rig = q(
            f"SELECT SUM(CASE WHEN restricao_contato_pessoas_pandemia = "
            f"'Ficou rigorosamente em casa' THEN peso_amostral ELSE 0 END) "
            f"/ SUM(peso_amostral) * 100 AS v FROM base_saude WHERE mes_ref = '{m}'"
        ).v[0]
        atend = q(
            f"SELECT SUM(CASE WHEN buscou_atendimento_saude = 'Sim' THEN peso_amostral ELSE 0 END) "
            f"/ SUM(peso_amostral) * 100 AS v FROM base_comportamento WHERE mes_ref = '{m}'"
        ).v[0]
        ho = q(
            f"SELECT SUM(CASE WHEN fez_home_office = 'Sim' THEN peso_amostral ELSE 0 END) "
            f"/ SUM(peso_amostral) * 100 AS v FROM base_comportamento "
            f"WHERE mes_ref = '{m}' AND trabalhou_na_semana = 'Sim'"
        ).v[0]
        linhas += [
            {"mes": MES_LABEL[m], "indicador": "Ficou rigorosamente em casa", "pct": rig},
            {"mes": MES_LABEL[m], "indicador": "Buscou atendimento de saúde", "pct": atend},
            {"mes": MES_LABEL[m], "indicador": "Home office (entre ocupados)", "pct": ho},
        ]
    return pd.DataFrame(linhas)


def busca_atendimento(meses) -> pd.DataFrame:
    geral = q(
        f"SELECT SUM(CASE WHEN buscou_atendimento_saude = 'Sim' THEN peso_amostral ELSE 0 END) "
        f"/ SUM(peso_amostral) * 100 AS v FROM base_comportamento WHERE {filtro_mes(meses)}"
    ).v[0]
    sint = q(
        f"""
        SELECT SUM(CASE WHEN c.buscou_atendimento_saude = 'Sim' THEN c.peso_amostral ELSE 0 END)
                   / SUM(c.peso_amostral) * 100 AS v
        FROM base_comportamento c
        JOIN base_saude s ON {join_cond('c', 's')}
        WHERE {filtro_mes(meses, 'c')} AND s.ind_teve_sintoma_gripal = 1
        """
    ).v[0]
    return pd.DataFrame(
        {"grupo": ["População geral", "Sintomáticos (síndrome gripal)"], "pct": [geral, sint]}
    )


def locais_atendimento(meses) -> pd.DataFrame:
    cols = ",\n".join(
        f"SUM(CASE WHEN s.{c} = 'Sim' THEN s.peso_amostral ELSE 0 END) "
        f"/ SUM(s.peso_amostral) * 100 AS \"{lab}\""
        for c, lab in LOCAIS_ATENDIMENTO.items()
    )
    df = q(
        f"""
        SELECT {cols}
        FROM base_saude s
        JOIN base_comportamento c ON {join_cond('s', 'c')}
        WHERE {filtro_mes(meses, 's')} AND c.buscou_atendimento_saude = 'Sim'
        """
    )
    return df.T.reset_index().set_axis(["local", "pct"], axis=1).sort_values("pct")


def situacao_mercado(meses) -> pd.DataFrame:
    return q(
        f"SELECT situacao_mercado_trabalho AS situacao, SUM(peso_amostral) AS pop "
        f"FROM base_comportamento "
        f"WHERE {filtro_mes(meses)} AND situacao_mercado_trabalho <> 'Nao se aplica' "
        f"GROUP BY situacao ORDER BY pop DESC"
    )


def home_office_regiao(meses) -> pd.DataFrame:
    return q(
        f"""
        SELECT l.regiao,
               SUM(CASE WHEN c.fez_home_office = 'Sim' THEN c.peso_amostral ELSE 0 END)
                   / SUM(c.peso_amostral) * 100 AS pct
        FROM base_comportamento c
        JOIN dim_localizacao l ON {join_cond('c', 'l')}
        WHERE {filtro_mes(meses, 'c')} AND c.trabalhou_na_semana = 'Sim'
        GROUP BY l.regiao ORDER BY pct
        """
    )


def motivos_afastamento(meses) -> pd.DataFrame:
    df = q(
        f"SELECT motivo_afastamento AS motivo, SUM(peso_amostral) AS pop "
        f"FROM base_comportamento "
        f"WHERE {filtro_mes(meses)} AND estava_afastado_com_vinculo = 'Sim' "
        f"GROUP BY motivo ORDER BY pop DESC LIMIT 6"
    )
    df["pct"] = df["pop"] / df["pop"].sum() * 100
    return df.sort_values("pct")


def cobertura_beneficios(meses) -> pd.DataFrame:
    cols = ",\n".join(
        f"SUM(CASE WHEN {c} = 'Sim' THEN peso_amostral ELSE 0 END) "
        f"/ SUM(peso_amostral) * 100 AS \"{lab}\""
        for c, lab in BENEFICIOS.items()
    )
    df = q(f"SELECT {cols} FROM base_economico WHERE {filtro_mes(meses)}")
    return df.T.reset_index().set_axis(["beneficio", "pct"], axis=1).sort_values("pct")


def evolucao_auxilio() -> pd.DataFrame:
    return q(
        """
        SELECT mes_ref AS mes_ref,
               SUM(CASE WHEN recebeu_auxilio_emergencial = 'Sim' THEN peso_amostral ELSE 0 END)
                   / SUM(peso_amostral) * 100 AS pct_ae,
               SUM(CASE WHEN recebeu_bolsa_familia = 'Sim' THEN peso_amostral ELSE 0 END)
                   / SUM(peso_amostral) * 100 AS pct_bf,
               SUM(CASE WHEN recebeu_auxilio_emergencial = 'Sim'
                         AND valor_auxilio_emergencial IS NOT NULL
                        THEN valor_auxilio_emergencial * peso_amostral ELSE 0 END)
               / NULLIF(SUM(CASE WHEN recebeu_auxilio_emergencial = 'Sim'
                                  AND valor_auxilio_emergencial IS NOT NULL
                                 THEN peso_amostral ELSE 0 END), 0) AS valor_medio
        FROM base_economico GROUP BY mes_ref ORDER BY mes_ref
        """
    )


def auxilio_por_regiao(meses):
    df = q(
        f"""
        SELECT l.regiao,
               SUM(e.valor_auxilio_emergencial * e.peso_amostral)
                   / SUM(e.peso_amostral) AS valor_medio
        FROM base_economico e
        JOIN dim_localizacao l ON {join_cond('e', 'l')}
        WHERE {filtro_mes(meses, 'e')}
          AND e.recebeu_auxilio_emergencial = 'Sim'
          AND e.valor_auxilio_emergencial IS NOT NULL
        GROUP BY l.regiao ORDER BY valor_medio
        """
    )
    geral = q(
        f"""
        SELECT SUM(valor_auxilio_emergencial * peso_amostral) / SUM(peso_amostral) AS v
        FROM base_economico
        WHERE {filtro_mes(meses)}
          AND recebeu_auxilio_emergencial = 'Sim'
          AND valor_auxilio_emergencial IS NOT NULL
        """
    ).v[0]
    return df, geral


def overlap_beneficios(meses) -> pd.DataFrame:
    return q(
        f"""
        SELECT
            CASE
                WHEN recebeu_auxilio_emergencial = 'Sim' AND recebeu_bolsa_familia = 'Sim'
                    THEN 'Auxílio Emergencial + Bolsa Família'
                WHEN recebeu_auxilio_emergencial = 'Sim' THEN 'Só Auxílio Emergencial'
                WHEN recebeu_bolsa_familia = 'Sim' THEN 'Só Bolsa Família'
                ELSE 'Nenhum dos dois'
            END AS grupo,
            SUM(peso_amostral) AS pop
        FROM base_economico WHERE {filtro_mes(meses)}
        GROUP BY grupo
        """
    )


def supressao_renda(meses) -> pd.DataFrame:
    df = q(
        f"SELECT ind_tem_renda_efetiva AS ind, SUM(peso_amostral) AS pop "
        f"FROM base_economico WHERE {filtro_mes(meses)} GROUP BY ind"
    )
    df["situacao"] = df["ind"].map({1: "Com renda efetiva", 0: "Sem renda efetiva"})
    df["pct"] = df["pop"] / df["pop"].sum() * 100
    return df.dropna(subset=["situacao"])


def distribuicao_renda(meses) -> pd.DataFrame:
    return q(
        f"SELECT renda_efetiva_trabalho_principal AS renda, peso_amostral AS peso "
        f"FROM base_economico "
        f"WHERE {filtro_mes(meses)} AND renda_efetiva_trabalho_principal > 0"
    )


def mediana_pond(valores: pd.Series, pesos: pd.Series) -> float:
    d = pd.DataFrame({"v": valores, "p": pesos}).sort_values("v")
    corte = d["p"].sum() / 2
    return float(d.loc[d["p"].cumsum() >= corte, "v"].iloc[0])


# =========================================================
# SIDEBAR — filtro de meses
# =========================================================
st.sidebar.header("Filtros")
meses_sel = st.sidebar.multiselect(
    "Meses de referência (gráficos consolidados)",
    options=MESES,
    default=MESES,
    format_func=lambda m: MES_LABEL[m],
)
if not meses_sel:
    meses_sel = MESES
    st.sidebar.warning("Nenhum mês selecionado — exibindo os três meses.")
st.sidebar.caption(
    "O filtro afeta os gráficos consolidados. As séries de **evolução mensal** "
    "sempre mostram set/out/nov 2020."
)

# =========================================================
# CABEÇALHO
# =========================================================
st.title("🦠 Análise Refinada — PNAD COVID-19")
st.markdown(
    "Estudo epidemiológico aplicado sobre a **PNAD COVID-19 (IBGE)** — meses de "
    "**setembro, outubro e novembro de 2020**, a fase de desaceleração da primeira onda. "
    "Gera insumos para recomendações hospitalares em caso de novo surto."
)

abas = st.tabs(
    [
        "🏠 Visão Geral",
        "1️⃣ Sintomas Clínicos",
        "2️⃣ Comportamento",
        "3️⃣ Características Econômicas",
        "✅ Conclusão e Recomendações",
    ]
)

# =========================================================
# ABA 0 — VISÃO GERAL
# =========================================================
with abas[0]:
    st.header("Visão Geral do Estudo")
    st.markdown(
        """
O objetivo é responder **3 perguntas temáticas** usando até 20 variáveis selecionadas da
PNAD COVID-19, gerando insumos para recomendações hospitalares em caso de novo surto.

| Tema | Perguntas respondidas |
|---|---|
| **1. Sintomas Clínicos** | Prevalência, severidade, testagem, comorbidades, perfil demográfico |
| **2. Comportamento da População** | Proteção, busca por atendimento, mercado de trabalho, isolamento |
| **3. Características Econômicas** | Renda, auxílios, proteção social, vulnerabilidade |

Os três meses (set/out/nov 2020) cobrem a **desaceleração da primeira onda**, permitindo
observar a evolução mês a mês dos indicadores.

> **Nota metodológica:** todas as agregações usam `peso_amostral` para representar
> corretamente a população brasileira.
"""
    )

    st.subheader("🗂️ Organização do Banco de Dados — Star Schema")
    st.markdown(
        """
A análise consolida **~1,15 milhão de entrevistados** distribuídos em 5 tabelas Parquet,
particionadas por `MES_REF` e ligadas pela chave
`uf + id_domicilio + id_morador + mes_entrevista`:

| Tabela | Conteúdo |
|---|---|
| `base_saude` | Sintomas, testagem COVID, comorbidades |
| `base_comportamento` | Isolamento, busca por atendimento, mercado de trabalho, home office |
| `base_economico` | Renda, Auxílio Emergencial, Bolsa Família, Seguro Desemprego |
| `dim_perfil` | Sexo, faixa etária, cor/raça, escolaridade |
| `dim_localizacao` | UF, região, capital, região metropolitana |
"""
    )

    st.subheader("📋 As 20 variáveis selecionadas")
    variaveis = pd.DataFrame(
        {
            "Variável": [
                "ind_teve_sintoma_gripal", "sintoma_febre", "sintoma_tosse",
                "sintoma_dificuldade_respirar", "sintoma_perda_olfato_paladar",
                "qtd_sintomas_relatados", "resultado_teste_positivo",
                "medico_deu_diagnostico_diabetes", "medico_deu_diagnostico_hipertensao",
                "medico_deu_diagnostico_doencas_coracao_infarto_angina",
                "restricao_contato_pessoas_pandemia", "buscou_atendimento_saude",
                "situacao_mercado_trabalho", "fez_home_office", "trabalhou_na_semana",
                "recebeu_auxilio_emergencial", "valor_auxilio_emergencial",
                "renda_efetiva_trabalho_principal", "recebeu_bolsa_familia",
                "recebeu_seguro_desemprego",
            ],
            "Tema": (
                ["Sintomas Clínicos"] * 10
                + ["Comportamento"] * 5
                + ["Econômico"] * 5
            ),
        }
    )
    st.dataframe(variaveis, width="stretch", hide_index=True)
    st.info(
        "A variável de proteção comportamental usada é `restricao_contato_pessoas_pandemia` — "
        "a PNAD COVID **não pergunta sobre uso de máscara**, e sim sobre o nível de isolamento "
        "social (4 níveis: sem restrição / reduziu contato / só sai p/ necessidades / "
        "rigorosamente em casa)."
    )

# =========================================================
# ABA 1 — SINTOMAS CLÍNICOS
# =========================================================
with abas[1]:
    st.header("1. Caracterização dos Sintomas Clínicos")
    st.markdown(
        "Analisa a **prevalência** de cada sintoma, a **severidade** (quantidade de sintomas), "
        "a **testagem**, as **comorbidades** e o **perfil demográfico** dos sintomáticos."
    )

    st.subheader("📊 Prevalência dos sintomas na população")
    df = prevalencia_sintomas(meses_sel)
    fig = px.bar(
        df, x="pct", y="sintoma", orientation="h", text="pct",
        title="Percentual da população que relatou cada sintoma",
        color="pct", color_continuous_scale="Reds",
    )
    fig.update_traces(texttemplate="%{text:.2f}%", textposition="outside")
    fig.update_layout(xaxis_title="% da população", yaxis_title="", coloraxis_showscale=False)
    st.plotly_chart(fig, width="stretch")

    st.subheader("📈 Evolução mensal dos sintomas")
    df = evolucao_sintomas()
    fig = px.line(
        df, x="mes", y="pct", color="sintoma", markers=True,
        title="Evolução da prevalência de cada sintoma (set → nov 2020)",
    )
    fig.update_layout(xaxis_title="Mês", yaxis_title="% da população")
    st.plotly_chart(fig, width="stretch")
    st.caption("A prevalência dos sintomas cai ao longo dos três meses — coerente com a "
               "desaceleração da primeira onda.")

    st.subheader("🔢 Quantidade de sintomas por pessoa")
    df = qtd_sintomas(meses_sel)
    fig = px.bar(
        df, x="qtd", y="pct", text="pct",
        title="Distribuição da população pela quantidade de sintomas relatados",
        color="pct", color_continuous_scale="Oranges",
    )
    fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
    fig.update_layout(xaxis_title="Quantidade de sintomas", yaxis_title="% da população",
                      coloraxis_showscale=False)
    st.plotly_chart(fig, width="stretch")

    st.subheader("🧪 Testagem e positividade")
    df = testagem(meses_sel)
    fig = px.bar(
        df, x="pct", y="indicador", orientation="h", text="pct",
        title="Indicadores de testagem para COVID-19", color="indicador",
    )
    fig.update_traces(texttemplate="%{text:.2f}%", textposition="outside")
    fig.update_layout(xaxis_title="%", yaxis_title="", showlegend=False)
    st.plotly_chart(fig, width="stretch")
    st.caption("A taxa de testagem é baixa no período, enquanto a positividade entre os "
               "testados é várias vezes maior que na população geral.")

    st.subheader("🩺 Comorbidades: população geral vs. testados positivos")
    df = comorbidades_geral_positivos(meses_sel)
    fig = px.bar(
        df, x="comorbidade", y="pct", color="grupo", barmode="group", text="pct",
        title="Prevalência de comorbidades — população geral vs. casos positivos",
    )
    fig.update_traces(texttemplate="%{text:.2f}%", textposition="outside")
    fig.update_layout(xaxis_title="", yaxis_title="% do grupo")
    st.plotly_chart(fig, width="stretch")

    st.subheader("👵 Comorbidades por faixa etária")
    df = comorbidades_por_faixa(meses_sel)
    fig = px.bar(
        df, x="faixa_etaria", y="pct", color="comorbidade", barmode="group", text="pct",
        category_orders={"faixa_etaria": ORDEM_FAIXA},
        title="Prevalência de cada comorbidade por faixa etária",
    )
    fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
    fig.update_layout(xaxis_title="Faixa etária", yaxis_title="% da faixa")
    st.plotly_chart(fig, width="stretch")
    st.caption("As comorbidades crescem fortemente a partir dos 45 anos — grupo que exige "
               "protocolo de triagem acelerado.")

    st.subheader("🧬 Perfil dos sintomáticos (síndrome gripal)")
    c1, c2 = st.columns(2)
    with c1:
        df = taxa_sintoma_por(meses_sel, "faixa_etaria")
        fig = px.bar(
            df, x="categoria", y="pct", text="pct",
            category_orders={"categoria": ORDEM_FAIXA},
            title="Taxa de síndrome gripal por faixa etária",
            color="pct", color_continuous_scale="Reds",
        )
        fig.update_traces(texttemplate="%{text:.2f}%", textposition="outside")
        fig.update_layout(xaxis_title="Faixa etária", yaxis_title="%", coloraxis_showscale=False)
        st.plotly_chart(fig, width="stretch")
    with c2:
        df = taxa_sintoma_por(meses_sel, "sexo")
        fig = px.bar(
            df, x="categoria", y="pct", text="pct",
            title="Taxa de síndrome gripal por sexo", color="categoria",
        )
        fig.update_traces(texttemplate="%{text:.2f}%", textposition="outside")
        fig.update_layout(xaxis_title="Sexo", yaxis_title="%", showlegend=False)
        st.plotly_chart(fig, width="stretch")

    df = taxa_sintoma_faixa_sexo(meses_sel)
    matriz = df.pivot(index="sexo", columns="faixa_etaria", values="pct").reindex(
        columns=ORDEM_FAIXA
    )
    fig = px.imshow(
        matriz, text_auto=".2f", color_continuous_scale="Reds", aspect="auto",
        title="Taxa de síndrome gripal — faixa etária × sexo",
    )
    fig.update_layout(xaxis_title="Faixa etária", yaxis_title="Sexo")
    st.plotly_chart(fig, width="stretch")

# =========================================================
# ABA 2 — COMPORTAMENTO
# =========================================================
with abas[2]:
    st.header("2. Comportamento da População")
    st.markdown(
        "Analisa a **adesão às medidas de proteção** (isolamento social), a **busca por "
        "atendimento**, a **situação no mercado de trabalho** e o **home office**."
    )

    st.subheader("🏠 Restrição de contato × situação no mercado de trabalho")
    df = isolamento_x_trabalho(meses_sel)
    fig = px.bar(
        df, x="situacao", y="pct", color="restricao", text="pct",
        category_orders={"restricao": list(LABEL_RESTR.values())},
        title="Nível de isolamento social por situação no mercado de trabalho (composição %)",
    )
    fig.update_traces(texttemplate="%{text:.1f}%", textposition="inside")
    fig.update_layout(xaxis_title="", yaxis_title="% do grupo", barmode="stack",
                      legend_title="Nível de restrição")
    st.plotly_chart(fig, width="stretch")
    st.caption("Trabalhadores ocupados presencialmente são os que menos aderiram ao "
               "isolamento estrito.")

    st.subheader("📈 Evolução mensal do comportamento")
    df = evolucao_comportamento()
    fig = px.line(
        df, x="mes", y="pct", color="indicador", markers=True,
        title="Isolamento, busca por atendimento e home office (set → nov 2020)",
    )
    fig.update_layout(xaxis_title="Mês", yaxis_title="%")
    st.plotly_chart(fig, width="stretch")

    st.subheader("🏥 Busca por atendimento de saúde")
    c1, c2 = st.columns(2)
    with c1:
        df = busca_atendimento(meses_sel)
        fig = px.bar(
            df, x="grupo", y="pct", text="pct", color="grupo",
            title="Busca por atendimento — geral vs. sintomáticos",
        )
        fig.update_traces(texttemplate="%{text:.2f}%", textposition="outside")
        fig.update_layout(xaxis_title="", yaxis_title="%", showlegend=False)
        st.plotly_chart(fig, width="stretch")
    with c2:
        df = locais_atendimento(meses_sel)
        fig = px.bar(
            df, x="pct", y="local", orientation="h", text="pct",
            title="Onde quem buscou atendimento foi atendido",
            color="pct", color_continuous_scale="Blues",
        )
        fig.update_traces(texttemplate="%{text:.2f}%", textposition="outside")
        fig.update_layout(xaxis_title="%", yaxis_title="", coloraxis_showscale=False)
        st.plotly_chart(fig, width="stretch")

    st.subheader("💼 Situação no mercado de trabalho")
    df = situacao_mercado(meses_sel)
    fig = px.pie(
        df, names="situacao", values="pop", hole=0.4,
        title="Distribuição da população por situação no mercado de trabalho",
    )
    fig.update_traces(textinfo="label+percent")
    st.plotly_chart(fig, width="stretch")

    st.subheader("🌎 Home office por região")
    df = home_office_regiao(meses_sel)
    fig = px.bar(
        df, x="pct", y="regiao", orientation="h", text="pct",
        title="Percentual de ocupados em home office por região",
        color="pct", color_continuous_scale="Greens",
    )
    fig.update_traces(texttemplate="%{text:.2f}%", textposition="outside")
    fig.update_layout(xaxis_title="% dos ocupados", yaxis_title="", coloraxis_showscale=False)
    st.plotly_chart(fig, width="stretch")
    st.caption("Norte e Nordeste registram as menores taxas de home office — maior exposição "
               "presencial.")

    st.subheader("📋 Motivos de afastamento do trabalho")
    df = motivos_afastamento(meses_sel)
    fig = px.bar(
        df, x="pct", y="motivo", orientation="h", text="pct",
        title="Principais motivos de afastamento entre quem tem vínculo",
        color="pct", color_continuous_scale="Purples",
    )
    fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
    fig.update_layout(xaxis_title="% dos afastados", yaxis_title="", coloraxis_showscale=False)
    st.plotly_chart(fig, width="stretch")

# =========================================================
# ABA 3 — CARACTERÍSTICAS ECONÔMICAS
# =========================================================
with abas[3]:
    st.header("3. Características Econômicas")
    st.markdown(
        "Analisa a **cobertura dos auxílios governamentais**, a **sobreposição de benefícios**, "
        "a **supressão de renda** e a **distribuição da renda do trabalho**."
    )

    st.subheader("💸 Cobertura dos benefícios sociais")
    df = cobertura_beneficios(meses_sel)
    fig = px.bar(
        df, x="pct", y="beneficio", orientation="h", text="pct",
        title="Percentual da população que recebeu cada benefício",
        color="pct", color_continuous_scale="Blues",
    )
    fig.update_traces(texttemplate="%{text:.2f}%", textposition="outside")
    fig.update_layout(xaxis_title="% da população", yaxis_title="", coloraxis_showscale=False)
    st.plotly_chart(fig, width="stretch")

    st.subheader("📈 Evolução do Auxílio Emergencial")
    df = evolucao_auxilio()
    df["mes"] = df["mes_ref"].map(MES_LABEL)
    c1, c2 = st.columns(2)
    with c1:
        cob = df.melt(
            id_vars="mes", value_vars=["pct_ae", "pct_bf"],
            var_name="beneficio", value_name="pct",
        )
        cob["beneficio"] = cob["beneficio"].map(
            {"pct_ae": "Auxílio Emergencial", "pct_bf": "Bolsa Família"}
        )
        fig = px.line(
            cob, x="mes", y="pct", color="beneficio", markers=True,
            title="Cobertura mensal (% da população)",
        )
        fig.update_layout(xaxis_title="Mês", yaxis_title="%")
        st.plotly_chart(fig, width="stretch")
    with c2:
        fig = px.bar(
            df, x="mes", y="valor_medio", text="valor_medio",
            title="Valor médio do Auxílio Emergencial (R$)",
            color="valor_medio", color_continuous_scale="Teal",
        )
        fig.update_traces(texttemplate="R$ %{text:.0f}", textposition="outside")
        fig.update_layout(xaxis_title="Mês", yaxis_title="R$", coloraxis_showscale=False)
        st.plotly_chart(fig, width="stretch")
    st.caption("O valor médio do auxílio recua ao longo dos meses — política pública sofreu "
               "redução no período.")

    st.subheader("🗺️ Valor médio do Auxílio Emergencial por região")
    df, valor_geral = auxilio_por_regiao(meses_sel)
    fig = px.bar(
        df, x="valor_medio", y="regiao", orientation="h", text="valor_medio",
        title="Valor médio ponderado do Auxílio Emergencial por região",
        color="valor_medio", color_continuous_scale="Teal",
    )
    fig.update_traces(texttemplate="R$ %{text:.0f}", textposition="outside")
    fig.add_vline(
        x=valor_geral, line_dash="dash", line_color="red",
        annotation_text=f"Média geral: R$ {valor_geral:.0f}",
    )
    fig.update_layout(xaxis_title="R$", yaxis_title="", coloraxis_showscale=False)
    st.plotly_chart(fig, width="stretch")

    st.subheader("🔗 Sobreposição: Auxílio Emergencial + Bolsa Família")
    df = overlap_beneficios(meses_sel)
    fig = px.pie(
        df, names="grupo", values="pop", hole=0.4,
        title="Grupos de beneficiários (sobreposição AE × BF)",
    )
    fig.update_traces(textinfo="label+percent")
    st.plotly_chart(fig, width="stretch")
    st.caption("A sobreposição AE + BF indica famílias em dupla vulnerabilidade social.")

    st.subheader("💔 Supressão de renda efetiva")
    df = supressao_renda(meses_sel)
    fig = px.bar(
        df, x="situacao", y="pct", text="pct", color="situacao",
        title="População com e sem renda efetiva do trabalho no período",
    )
    fig.update_traces(texttemplate="%{text:.2f}%", textposition="outside")
    fig.update_layout(xaxis_title="", yaxis_title="% da população", showlegend=False)
    st.plotly_chart(fig, width="stretch")

    st.subheader("💰 Distribuição da renda efetiva do trabalho principal")
    df = distribuicao_renda(meses_sel)
    med = mediana_pond(df["renda"], df["peso"])
    media = float((df["renda"] * df["peso"]).sum() / df["peso"].sum())
    df_plot = df[df["renda"] <= 8000]
    fig = px.histogram(
        df_plot, x="renda", y="peso", histfunc="sum", nbins=60,
        title="Distribuição ponderada da renda efetiva (truncada em R$ 8.000)",
    )
    fig.add_vline(x=med, line_dash="dash", line_color="red",
                  annotation_text=f"Mediana: R$ {med:,.0f}")
    fig.add_vline(x=media, line_dash="dash", line_color="orange",
                  annotation_text=f"Média: R$ {media:,.0f}")
    fig.add_vline(x=SALARIO_MINIMO_2020, line_dash="dot", line_color="green",
                  annotation_text=f"Salário mínimo 2020: R$ {SALARIO_MINIMO_2020:,.0f}")
    fig.update_layout(xaxis_title="Renda efetiva (R$)", yaxis_title="População ponderada")
    st.plotly_chart(fig, width="stretch")

# =========================================================
# ABA 4 — CONCLUSÃO E RECOMENDAÇÕES
# =========================================================
with abas[4]:
    st.header("Conclusão — Recomendações ao Hospital")
    st.markdown(
        "Com base nas três análises (set/out/nov 2020 — fase de desaceleração da primeira "
        "onda), as recomendações a seguir são **derivadas dos dados** da PNAD COVID-19."
    )
    st.warning(
        "**Nota metodológica:** a PNAD COVID **não pergunta sobre uso de máscara**. O indicador "
        "comportamental disponível é o **nível de restrição de contato social** (4 níveis). "
        "As recomendações de prevenção refletem esse fato."
    )

    # --- números calculados ao vivo, para o texto bater com os gráficos ---
    prev = prevalencia_sintomas(MESES).set_index("sintoma")["pct"]
    tst = testagem(MESES).set_index("indicador")["pct"]
    ba = busca_atendimento(MESES).set_index("grupo")["pct"]
    rig_geral = q(
        "SELECT SUM(CASE WHEN restricao_contato_pessoas_pandemia = 'Ficou rigorosamente em casa' "
        "THEN peso_amostral ELSE 0 END) / SUM(peso_amostral) * 100 AS v FROM base_saude"
    ).v[0]
    sint_geral = q(
        "SELECT SUM(ind_teve_sintoma_gripal * peso_amostral) / SUM(peso_amostral) * 100 AS v "
        "FROM base_saude"
    ).v[0]
    ae_geral = q(
        "SELECT SUM(CASE WHEN recebeu_auxilio_emergencial = 'Sim' THEN peso_amostral ELSE 0 END) "
        "/ SUM(peso_amostral) * 100 AS v FROM base_economico"
    ).v[0]
    sup = supressao_renda(MESES).set_index("situacao")["pct"]
    sem_renda = float(sup.get("Sem renda efetiva", 0))

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Síndrome gripal (população)", f"{sint_geral:.1f}%")
    k2.metric("Buscou atendimento (sintomáticos)", f"{ba['Sintomáticos (síndrome gripal)']:.1f}%")
    k3.metric("Ficou rigorosamente em casa", f"{rig_geral:.1f}%")
    k4.metric("Sem renda efetiva", f"{sem_renda:.1f}%")

    st.markdown("---")
    st.subheader("Eixo 1 — Triagem e Priorização Clínica")
    st.markdown(
        f"""
| Ação | Fundamentação nos dados |
|---|---|
| Priorizar triagem de pacientes com **febre + tosse combinadas** | Sintomas mais prevalentes ({prev.get('Febre', 0):.1f}% e {prev.get('Tosse', 0):.1f}%) e co-ocorrentes nos 3 meses |
| Sinalizar **alto risco** em pacientes com dificuldade respiratória | Indicador de severidade clínica imediata |
| Criar protocolo acelerado para **idosos com hipertensão ou diabetes** | Comorbidades crescem rapidamente após os 45 anos |
| Usar **perda de olfato/paladar** como critério de isolamento imediato | Sintoma altamente específico da COVID-19 |
"""
    )

    st.subheader("Eixo 2 — Dimensionamento de Capacidade")
    st.markdown(
        f"""
| Indicador | Valor observado (3 meses) | Implicação |
|---|---|---|
| Busca por atendimento entre sintomáticos | {ba['Sintomáticos (síndrome gripal)']:.1f}% | Estimar demanda real × capacidade instalada |
| Positividade entre testados | {tst['Positivos entre os testados']:.1f}% | Reservar leitos proporcionalmente |
| Síndrome gripal na população | {sint_geral:.1f}% | Base para previsão de fluxo de emergência |
| Evolução mensal | Queda de set → nov | Capacidade pode ser escalonada conforme a curva |
"""
    )

    st.subheader("Eixo 3 — Prevenção e Comunicação")
    st.markdown(
        f"""
| Ação | Fundamentação nos dados |
|---|---|
| **Incentivar permanência em casa** na próxima onda | Apenas {rig_geral:.1f}% ficou rigorosamente em casa; a maioria reduziu contato mas continuou saindo |
| Campanha focada em **trabalhadores presenciais** | Menor adesão ao isolamento estrito nesse grupo |
| Priorizar conscientização no **Norte e Nordeste** | Menor taxa de home office, maior exposição |
| Reforçar **testagem precoce** | Apenas {tst['Realizou teste de COVID']:.1f}% da população se testou no período |
"""
    )

    st.subheader("Eixo 4 — Suporte Socioeconômico")
    st.markdown(
        f"""
| Ação | Fundamentação nos dados |
|---|---|
| Mapear pacientes **sem renda efetiva** para suporte assistencial | {sem_renda:.1f}% dos entrevistados sem renda efetiva no mês |
| Articular o hospital com a rede de proteção social (CRAS, CREAS) | Dependência de Auxílio Emergencial em torno de {ae_geral:.1f}% |
| Considerar fila para beneficiários de **BF + AE** (dupla vulnerabilidade) | Sobreposição expressiva entre os dois benefícios |
| Monitorar a **queda do valor médio do auxílio** ao longo dos meses | Redução da política pública agrava a vulnerabilidade |
"""
    )

    st.subheader("Eixo 5 — Ação Territorial")
    st.markdown(
        """
| Ação | Fundamentação nos dados |
|---|---|
| Monitorar **Norte e Nordeste**: menor renda, menor home office, maior informalidade | Confluência de fatores de risco |
| Expandir **testagem móvel** em áreas rurais | Menor acesso a estabelecimentos de saúde |
| Integrar dados de localização para identificar **clusters de alta positividade** | A chave uf + id_domicilio permite análise geoespacial |
"""
    )

    st.info(
        "**Limitação metodológica:** esta análise cobre 3 meses (set/out/nov 2020) — a fase de "
        "desaceleração da primeira onda. Para um surto futuro, recomenda-se complementar com "
        "dados de internação hospitalar e óbitos do SIVEP-Gripe e estender a série temporal "
        "para captar a sazonalidade plena."
    )
