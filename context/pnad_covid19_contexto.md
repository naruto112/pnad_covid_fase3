# PNAD COVID-19 — Pipeline de Engenharia de Dados

## Contexto e objetivo

Analisar os microdados da PNAD COVID-19 (IBGE) para entender o comportamento da população durante a pandemia e identificar indicadores úteis para planejamento em caso de novo surto.

A pesquisa cobre **maio a novembro de 2020**, com ~193 mil domicílios por mês em **amostra fixa** — as mesmas famílias respondem ao longo do tempo, formando um painel longitudinal. Isso permite rastrear evolução de renda, sintomas e situação de trabalho na mesma família mês a mês.

**Fonte dos dados:** [FTP IBGE — PNAD COVID-19](https://ftp.ibge.gov.br/Trabalho_e_Rendimento/Pesquisa_Nacional_por_Amostra_de_Domicilios_PNAD_COVID19/Microdados/)

---

## Arquivos do projeto

| Arquivo | Função |
|---|---|
| `raw/dicionario_variaveis.csv` | 148 variáveis: `codigo_variavel → nome_semantico`, bloco temático, tipo de dado, domínio, seção do questionário e meses disponíveis |
| `raw/dicionario_categorias.csv` | 639 entradas: par `(codigo_variavel, valor_inteiro) → descricao_valor`. Cobre todas as variáveis categóricas; variáveis contínuas (renda, horas) têm campo `dominio` preenchido e **não** aparecem aqui com valores enumerados |
| `raw/dicionario_pnad_covid_consolidado.xlsx` | Versão Excel com as duas abas acima — fonte de verdade para revisão manual |
| `raw/PNAD_COVID_*.csv` | Microdados brutos do IBGE (encoding latin1, separador vírgula) |
| `code/raw_to_trusted.ipynb` | Pipeline principal: ingestão → renomeação → tradução de categorias → dimensões → bases temáticas → Parquet particionado |

---

## Decisões de engenharia

### Dois dicionários como entrada externa

O pipeline não tem nenhum nome de coluna nem nenhum rótulo hardcoded.

- **`dicionario_variaveis.csv`** controla renomeação de colunas, cast de tipos e agrupamento por bloco temático.
- **`dicionario_categorias.csv`** controla a tradução de valores numéricos para texto legível (ex.: `1 → "Sim"`, `2 → "Não"`, `1 → "Homem"`, `2 → "Mulher"`).

Para adicionar ou remover variáveis, basta editar os CSVs — o código não muda.

### Tradução de categorias dirigida por dicionário

Em vez de `F.when(F.col('sexo') == 1, 'Masculino')...`, o pipeline:
1. Lê `dicionario_categorias.csv` no driver (Pandas) e constrói um dict `{nome_semantico: {int_valor: label}}`.
2. Para cada variável categórica, aplica `F.create_map()` em Spark — sem UDFs, dentro do otimizador nativo.
3. Variáveis contínuas (campo `dominio` preenchido em `dicionario_variaveis.csv`, ex.: "valor em reais") são puladas — valores numéricos ficam intactos para cálculo.

### Por que Pandas para os dicionários e Spark para os microdados

Os dois dicionários têm juntos ~800 linhas e precisam estar disponíveis antes de o Spark montar seu plano de execução (lazy evaluation). Pandas lê de forma eagerly no driver; colocar os dicionários no Spark seria overhead sem ganho.

### Renomeação de colunas em passagem única

```python
df.select([F.col(codigo).alias(nome_semantico) for codigo in codigos_validos])
```

Seleção e renomeação em um único `.select()`, sem DataFrames intermediários.

### Tipagem dirigida pelo dicionário

A coluna `tipo` no `dicionario_variaveis.csv` (`integer`, `double`, `string`) controla o cast de cada variável. Sem `inferSchema` — inferência automática em arquivos grandes é lenta e propensa a erro.

### Encoding latin1

Herança histórica dos sistemas do governo brasileiro. O risco é que o Spark lendo como UTF-8 corrompe silenciosamente os caracteres sem lançar erro. O Parquet de saída já normaliza tudo para UTF-8 — o problema fica contido na camada de ingestão.

### Peso amostral obrigatório

Toda agregação usa `V1032` (`peso_amostral`). Sem ponderar, os números representam a amostra, não o Brasil.

```python
# Correto — estima % da população brasileira
(F.sum(F.col("ind_teve_sintoma_gripal") * F.col("peso_amostral")) /
 F.sum("peso_amostral") * 100).alias("pct_com_sintoma")
```

---

## As 4 bases temáticas

Todas compartilham a mesma chave de identificação para garantir JOIN entre elas a qualquer momento:

> **Chave:** `uf + id_domicilio + id_morador + mes_entrevista`

### BASE_SAUDE (36 variáveis)

Sintomas clínicos, busca por atendimento, internação, testagem, comorbidades pré-existentes e restrição de contato social.

Indicadores derivados: `ind_teve_sintoma_gripal` (1 se teve febre, tosse, dificuldade respirar, fadiga ou perda de olfato/paladar), `qtd_sintomas_relatados`.

Responde: quem ficou doente? Foi internado? Tinha comorbidades? Fez teste?

### BASE_COMPORTAMENTO (13 variáveis)

Busca de atendimento médico, uso de máscara, plano de saúde (bloco B), situação de trabalho na semana (se trabalhou, se estava afastado, se fez home office, se procurou trabalho).

Indicadores derivados: `situacao_mercado_trabalho` (classifica cada pessoa como Ocupado-trabalhou, Ocupado-afastado, Desocupado ou Fora da força de trabalho).

Responde: quem usou máscara? Quem buscou atendimento e onde? Quem conseguiu fazer home office?

### BASE_ECONOMICO (13 variáveis)

Tipo de vínculo empregatício, horas trabalhadas, renda habitual vs. efetiva, INSS, Bolsa Família, seguro-desemprego, auxílio emergencial, aposentadoria/pensão.

Indicadores derivados: `variacao_renda_reais` (renda efetiva − habitual), `ind_perdeu_renda`.

Responde: quem perdeu renda? Quem dependeu de auxílio emergencial?

### BASE_TRABALHO (32 variáveis)

Tipo de trabalho, área de atuação, tempo de afastamento, múltiplos empregos, tamanho do estabelecimento, contrato suspenso, desejo de trabalhar mais horas, formas e faixas de rendimento habitual e efetivo, home office/teletrabalho, motivo de não procurar trabalho.

Responde: como era a situação detalhada do mercado de trabalho durante a pandemia?

---

## Modelagem relacional — Star Schema

```
DIM_TEMPO ──────────────────┐
DIM_PERFIL ─────────────────┤
DIM_LOCALIZACAO ────────────┼──→ BASES TEMÁTICAS (saude, comportamento, economico, trabalho)
DIM_DICIONARIO (metadado) ──┘
```

**Grão:** 1 linha = 1 pessoa × 1 mês de entrevista.

| Tabela | Tipo | Conteúdo |
|---|---|---|
| `DIM_TEMPO` | Dimensão | `mes_entrevista`, `num_entrevista`, `fase_pandemia` |
| `DIM_PERFIL` | Dimensão | `faixa_etaria` (derivada de `idade`), `sexo`, `cor_raca`, `escolaridade` e demais vars do bloco `perfil` — todas com labels traduzidos |
| `DIM_LOCALIZACAO` | Dimensão | `uf`, `regiao` (derivada), `situacao_domicilio` ("Urbana"/"Rural"), `mora_na_capital` (nome da capital ou null), `mora_em_regiao_metropolitana` (nome da RM ou null) |
| `DIM_DICIONARIO` | Metadado | `codigo_variavel`, `nome_semantico`, `descricao_variavel`, `bloco`, `tipo`, `dominio`, `parte`, `meses` — serve como data catalog integrado |
| `BASE_SAUDE` | Temática | 36 vars do bloco `saude` + indicadores derivados |
| `BASE_COMPORTAMENTO` | Temática | 13 vars do bloco `comportamento` + `situacao_mercado_trabalho` |
| `BASE_ECONOMICO` | Temática | 13 vars do bloco `economico` + `variacao_renda_reais` + `ind_perdeu_renda` |
| `BASE_TRABALHO` | Temática | 32 vars do bloco `trabalho`, labels traduzidos |

**Por que Star Schema e não tabelas planas?** Em 7 meses × 193 mil domicílios, evitar redundância de strings como `"Superior Completo"` em cada linha faz diferença real de armazenamento e performance de query.

**DIM_DICIONARIO:** não tem FK física nas bases. Serve para linhagem — rastrear de onde cada coluna veio sem depender de documentação externa.

---

## Como empilhar novos meses

Muda só o `CONFIG` no topo do pipeline:

```python
CONFIG["caminho_microdados"] = "raw/PNAD_COVID_102020.csv"
CONFIG["mes_referencia"]     = "2020-10"
```

O `partitionBy("MES_REF")` com `spark.sql.sources.partitionOverwriteMode = dynamic` garante que cada mês fique em sua partição sem sobrescrever os anteriores. Para ler a série completa:

```python
df_serie = spark.read.parquet("output/pnad_covid/base_saude")

# Evolução temporal
df_serie.groupBy("MES_REF").agg(
    (F.sum(F.col("ind_teve_sintoma_gripal") * F.col("peso_amostral")) /
     F.sum("peso_amostral") * 100).alias("pct_sintoma")
).orderBy("MES_REF").show()
```

---

## Variáveis com semântica alterada no novo dicionário

| Variável | Código | Observação |
|---|---|---|
| `area_urbana_rural` | A007 | No novo dicionário, A007 mapeia questão sobre atividades escolares remotas, não área geográfica. `DIM_LOCALIZACAO` usa `situacao_domicilio` (V1022) para classificação Urbana/Rural. |
| `mora_na_capital` | CAPITAL | Mapeado para o nome da capital estadual quando o domicílio está nela; null caso contrário. |
| `mora_em_regiao_metropolitana` | RM_RIDE | Mapeado para o nome da região metropolitana; null caso contrário. |

---

## Estrutura de diretórios

```
projeto/
├── raw/
│   ├── PNAD_COVID_092020.csv
│   ├── PNAD_COVID_102020.csv
│   ├── PNAD_COVID_112020.csv
│   ├── dicionario_variaveis.csv          ← 148 variáveis com metadados
│   ├── dicionario_categorias.csv         ← 639 pares (variável, valor) → rótulo
│   └── dicionario_pnad_covid_consolidado.xlsx
├── context/
│   ├── pnad_covid19_contexto.md          ← este arquivo
│   └── CONTEXTO_DICIONARIO_PNAD_COVID.md
├── code/
│   └── raw_to_trusted.ipynb              ← pipeline principal
└── output/
    └── pnad_covid/
        ├── dim_tempo/        MES_REF=2020-11/
        ├── dim_perfil/       MES_REF=2020-11/
        ├── dim_localizacao/  MES_REF=2020-11/
        ├── dim_dicionario/
        ├── base_saude/       MES_REF=2020-11/
        ├── base_comportamento/ MES_REF=2020-11/
        ├── base_economico/   MES_REF=2020-11/
        └── base_trabalho/    MES_REF=2020-11/
```

---

## Dependências

```bash
pip install pyspark pandas openpyxl xlrd setuptools
```

> **Nota:** Python 3.12 removeu `distutils` da stdlib. O `setuptools` fornece a shim de compatibilidade necessária para o PySpark 3.5.x.
>
> Os microdados e dicionários do IBGE usam encoding `latin1`. O pipeline trata isso na ingestão — todas as saídas em Parquet são UTF-8.
