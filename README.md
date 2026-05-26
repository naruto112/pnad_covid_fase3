# Documentação — Pipeline PNAD COVID-19

## Visão geral

E ste pipeline transforma os microdados brutos da **PNAD COVID-19 (IBGE)** em bases analíticas limpas, com variáveis renomeadas semanticamente e categorias traduzidas de código numérico para texto legível (ex.: `1 → "Homem"`, `2 → "Mulher"`).

**Período coberto:** maio a novembro de 2020  
**Amostra:** ~193 mil domicílios por mês, painel fixo (as mesmas famílias são entrevistadas todos os meses, permitindo análise longitudinal)  
**Fonte:** [FTP IBGE — PNAD COVID-19](https://ftp.ibge.gov.br/Trabalho_e_Rendimento/Pesquisa_Nacional_por_Amostra_de_Domicilios_PNAD_COVID19/Microdados/)

---

## Arquitetura do pipeline

```
raw/PNAD_COVID_*.csv          (microdados brutos, latin1)
        │
        ▼
raw_to_trusted.ipynb          (PySpark)
        │
        ├── Dicionários de entrada
        │     ├── dicionario_variaveis.csv   → renomeação + tipos + blocos temáticos
        │     └── dicionario_categorias.csv  → tradução de códigos numéricos para labels
        │
        ▼
output/pnad_covid/             (Parquet, UTF-8, particionado por MES_REF)
        ├── dim_tempo/
        ├── dim_perfil/
        ├── dim_localizacao/
        ├── dim_dicionario/
        ├── base_saude/
        ├── base_comportamento/
        ├── base_economico/
        └── base_trabalho/
```

### Fluxo de transformação

| Etapa | O que acontece |
|---|---|
| **Ingestão** | Lê o CSV com encoding `latin1` e separador `,`; adiciona coluna `MES_REF` |
| **Renomeação** | Troca todos os códigos IBGE (`C003`, `B0011`…) pelos nomes semânticos do dicionário |
| **Tipagem** | Converte colunas para `integer` ou `double` conforme o dicionário (sem inferência automática) |
| **Tradução** | Substitui valores numéricos categóricos pelos rótulos do dicionário via `F.create_map()` |
| **Derivação** | Cria indicadores calculados (`ind_teve_sintoma_gripal`, `situacao_mercado_trabalho`, etc.) |
| **Persistência** | Grava Parquet particionado por `MES_REF`; cada execução sobrescreve só o mês processado |

---

## Chave de identificação (JOIN key)

Todas as bases compartilham a mesma chave de 4 colunas que identifica univocamente uma pessoa em um mês:

```
uf  +  id_domicilio  +  id_morador  +  mes_entrevista
```

Use esta chave para cruzar qualquer base com outra (ex.: unir `BASE_SAUDE` com `DIM_PERFIL` para ver sintomas por faixa etária).

> **Atenção:** `uf` está no formato nome do estado (`"São Paulo"`, `"Minas Gerais"`) após a tradução do dicionário. Certifique-se de usar o mesmo valor nas duas tabelas ao fazer o JOIN.

---

## Peso amostral — regra obrigatória

A PNAD é uma amostra complexa. **Nunca calcule percentuais ou médias sem ponderar pelo `peso_amostral`** (coluna presente em todas as bases temáticas). Sem ponderação, os números representam apenas a amostra coletada, não o Brasil.

```python
# Correto — % da população brasileira com febre na semana
(df_saude
    .groupBy('MES_REF')
    .agg(
        (F.sum(F.col('ind_teve_sintoma_gripal') * F.col('peso_amostral')) /
         F.sum('peso_amostral') * 100).alias('pct_sintoma')
    )
    .orderBy('MES_REF')
    .show())
```

---

## Tabelas de saída

### Dimensões

As dimensões descrevem os atributos de contexto de cada entrevistado. São a base para estratificar qualquer análise por perfil, localização ou tempo.

---

#### `DIM_TEMPO` — 4 colunas

Granularidade: 1 linha por combinação única de mês × número de entrevista.

| Coluna | Tipo | Descrição |
|---|---|---|
| `mes_entrevista` | string | Mês da pesquisa (5 a 11) |
| `num_entrevista` | string | Número da entrevista no domicílio |
| `fase_pandemia` | string | `Inicio da Pandemia` (mai–jun), `Pico da Primeira Onda` (jul–set), `Desaceleracao` (out–nov) |
| `MES_REF` | string | Partição: ano-mês do arquivo processado (ex.: `2020-11`) |

---

#### `DIM_PERFIL` — 23 colunas

Perfil demográfico e escolar de cada morador. Todas as colunas categóricas já contêm o texto legível.

| Coluna | Tipo | Exemplo de valor |
|---|---|---|
| `uf` | string | `"São Paulo"`, `"Minas Gerais"` |
| `id_domicilio` | string | Chave de JOIN |
| `id_morador` | string | Chave de JOIN |
| `mes_entrevista` | string | Chave de JOIN |
| `sexo` | string | `"Homem"`, `"Mulher"` |
| `cor_raca` | string | `"Branca"`, `"Preta"`, `"Parda"`, `"Amarela"`, `"Indígena"` |
| `escolaridade` | string | `"Sem instrução"` → `"Pós-graduação, mestrado ou doutorado"` |
| `idade` | string | Valor numérico em anos (contínua — não traduzida) |
| `faixa_etaria` | string | `"00-17"`, `"18-29"`, `"30-44"`, `"45-59"`, `"60-74"`, `"75+"` |
| `condicao_domicilio` | string | `"Pessoa responsável pelo domicílio"`, `"Cônjuge"`, etc. |
| `mora_na_capital` | string | Nome da capital estadual ou `null` |
| `mora_em_regiao_metropolitana` | string | Nome da RM/RIDE ou `null` |
| `tem_plano_saude` | string | `"Sim"`, `"Não"` (variável de frequência escolar no novo questionário) |
| `area_urbana_rural` | string | Questão escolar no novo dicionário (ver nota abaixo) |
| `MES_REF` | string | Partição |

> **Nota sobre `area_urbana_rural`:** no dicionário atual, a variável A007 foi reaproveitada para uma questão sobre atividades escolares remotas. Para classificação geográfica urbana/rural, use `situacao_domicilio` na `DIM_LOCALIZACAO`.

---

#### `DIM_LOCALIZACAO` — 9 colunas

Localização geográfica do domicílio.

| Coluna | Tipo | Exemplo de valor |
|---|---|---|
| `uf` | string | `"São Paulo"`, `"Bahia"` |
| `id_domicilio` | string | Chave de JOIN |
| `id_morador` | string | Chave de JOIN |
| `mes_entrevista` | string | Chave de JOIN |
| `situacao_domicilio` | string | `"Urbana"`, `"Rural"` |
| `mora_na_capital` | string | `"Município de São Paulo (SP)"` ou `null` |
| `mora_em_regiao_metropolitana` | string | `"Região Metropolitana de São Paulo (SP)"` ou `null` |
| `regiao` | string | `"Norte"`, `"Nordeste"`, `"Sudeste"`, `"Sul"`, `"Centro-Oeste"` |
| `MES_REF` | string | Partição |

---

#### `DIM_DICIONARIO` — 8 colunas

Catálogo de dados: documenta cada uma das 148 variáveis do pipeline. Não tem FK física — use para consulta de linhagem.

| Coluna | Descrição |
|---|---|
| `codigo_variavel` | Código original IBGE (`B0011`, `C003`, etc.) |
| `nome_semantico` | Nome usado nas bases de saída (`sintoma_febre`, `motivo_afastamento`) |
| `descricao_variavel` | Pergunta original do questionário |
| `bloco` | Grupo temático: `saude`, `comportamento`, `economico`, `trabalho`, `perfil`, `id`, `peso`, `rendimento`, `emprestimos`, `habitacao` |
| `tipo` | `integer`, `double` ou `string` |
| `dominio` | Para variáveis contínuas: descrição do domínio (`"valor em reais"`, `"01 a 30"`) |
| `parte` | Seção do questionário IBGE |
| `meses` | Meses em que a variável está disponível (`092020,102020,112020`) |

---

### Bases temáticas

As bases temáticas são o coração analítico do pipeline. Cada uma cobre um ângulo diferente da pandemia e pode ser cruzada com qualquer dimensão via a chave de JOIN.

Todas as bases incluem as colunas de identificação (`id`, `peso`) e a coluna de partição `MES_REF`.

---

#### `BASE_SAUDE` — 54 colunas

**Foco:** sintomas clínicos, busca por atendimento, internação, testagem e comorbidades pré-existentes.

**Variáveis-chave:**

| Coluna | Tipo | Valores possíveis |
|---|---|---|
| `sintoma_febre` | string | `"Sim"`, `"Não"`, `"Não sabe"`, `"Ignorado"` |
| `sintoma_tosse` | string | idem |
| `sintoma_dificuldade_respirar` | string | idem |
| `sintoma_fadiga` | string | idem |
| `sintoma_perda_olfato_paladar` | string | idem |
| `sintoma_dor_cabeca` / `_peito` / `_garganta` / `_muscular` / `_olhos` / `_nausea` / `_nariz_entupido` | string | idem |
| `semana_passada_teve_diarreia` | string | idem |
| `foi_sedado_intubado` | string | `"Sim"`, `"Não"` (internação) |
| `fez_teste_covid` | string | `"Sim"`, `"Não"` |
| `resultado_teste_positivo` | string | `"Positivo (sim)"`, `"Negativo (não)"`, etc. |
| `medico_deu_diagnostico_diabetes` | string | `"Sim"`, `"Não"` |
| `medico_deu_diagnostico_hipertensao` | string | idem |
| `medico_deu_diagnostico_asma_bronquite_enfisema_doencas` | string | idem |
| `medico_deu_diagnostico_doencas_coracao_infarto_angina` | string | idem |
| `medico_deu_diagnostico_depressao` | string | idem |
| `medico_deu_diagnostico_cancer` | string | idem |
| `local_buscou_atendimento_posto_saude_unidade_basica_saude` | string | `"Sim"`, `"Não"` |
| `local_buscou_atendimento_hospital_sus` | string | idem |
| `local_buscou_atendimento_hospital_privado_ligado_forcas` | string | idem |

**Indicadores derivados:**

| Coluna | Tipo | Descrição |
|---|---|---|
| `ind_teve_sintoma_gripal` | integer | `1` se reportou febre, tosse, dificuldade respirar, fadiga ou perda de olfato/paladar; `0` caso contrário |
| `qtd_sintomas_relatados` | integer | Contagem de sintomas gripais positivos (0 a 5) |

**Perguntas que responde:**
- Qual % da população teve sintomas gripais em cada mês?
- Quem ficou internado ou foi intubado?
- Quem tinha comorbidades (diabetes, hipertensão, doenças cardíacas)?
- Quem fez teste e qual foi o resultado?
- Onde as pessoas buscaram atendimento (UBS, SUS, privado)?

---

#### `BASE_COMPORTAMENTO` — 30 colunas

**Foco:** comportamentos adotados durante a pandemia — uso de máscara, busca por atendimento, situação no mercado de trabalho na semana da entrevista.

**Variáveis-chave:**

| Coluna | Tipo | Valores possíveis |
|---|---|---|
| `buscou_atendimento_saude` | string | `"Sim"`, `"Não"` |
| `atendimento_posto_ubs` | string | `"Sim"`, `"Não"` |
| `atendimento_ps_publico` / `_hospital_publico` / `_ps_privado` / `_hospital_privado` | string | idem |
| `usa_mascara` | string | `"Sim"`, `"Não"` |
| `tem_plano_saude_bloco_b` | string | `"Sim"`, `"Não"` |
| `trabalhou_na_semana` | string | `"Sim"`, `"Não"` |
| `estava_afastado_com_vinculo` | string | `"Sim"`, `"Não"` |
| `motivo_afastamento` | string | Ex.: `"Licença médica"`, `"Férias"`, `"Pandemia"` |
| `fez_home_office` | string | `"Sim"`, `"Não"` |
| `procurou_trabalho_semana` | string | `"Sim"`, `"Não"` |

**Indicador derivado:**

| Coluna | Tipo | Descrição |
|---|---|---|
| `situacao_mercado_trabalho` | string | `"Ocupado - trabalhou"` / `"Ocupado - afastado"` / `"Desocupado"` / `"Fora da forca de trabalho"` / `"Nao se aplica"` |

**Perguntas que responde:**
- Qual % da população usava máscara?
- Quem conseguiu fazer home office e quem não conseguiu?
- Como a situação no mercado de trabalho evoluiu mês a mês?
- Quem buscou atendimento médico e em qual tipo de serviço?

---

#### `BASE_ECONOMICO` — 30 colunas

**Foco:** renda efetiva recebida, auxílios governamentais, benefícios e vínculo empregatício.

**Variáveis-chave:**

| Coluna | Tipo | Descrição |
|---|---|---|
| `tem_carteira_assinada` | string | Tipo de vínculo empregatício |
| `horas_habituais_trabalhadas` | string | Faixa de horas normalmente trabalhadas por semana |
| `horas_efetivas_trabalhadas` | string | Faixa de horas efetivamente trabalhadas na semana |
| `contribui_inss` | string | `"Sim"`, `"Não"` |
| `renda_efetiva_trabalho_principal` | double | Valor em R$ efetivamente recebido na semana (0 a ~280.000) |
| `recebeu_auxilio_emergencial` | string | `"Sim"`, `"Não"` |
| `valor_auxilio_emergencial` | double | Valor em R$ recebido de auxílio emergencial |
| `recebeu_bolsa_familia` | string | `"Sim"`, `"Não"` |
| `valor_bolsa_familia` | double | Valor em R$ |
| `recebeu_seguro_desemprego` | string | `"Sim"`, `"Não"` |
| `recebeu_aposentadoria_pensao` | string | `"Sim"`, `"Não"` |
| `valor_aposentadoria_pensao` | double | Valor em R$ |

**Indicador derivado:**

| Coluna | Tipo | Descrição |
|---|---|---|
| `ind_tem_renda_efetiva` | integer | `1` se `renda_efetiva_trabalho_principal > 0`; `0` caso contrário |

> **Nota técnica:** `renda_habitual_trabalho_principal` (C011A) é um **indicador de participação** (valor fixo = 1) no questionário IBGE, não o valor da renda habitual em reais. Para comparar renda habitual com efetiva, use `valor_dinheiro` da `BASE_TRABALHO` (renda habitual em R$) e `renda_efetiva_trabalho_principal` desta base.

**Perguntas que responde:**
- Quem dependeu de auxílio emergencial?
- Qual a distribuição de renda efetiva entre os ocupados?
- Quem recebeu Bolsa Família ou aposentadoria durante a pandemia?

---

#### `BASE_TRABALHO` — 48 colunas

**Foco:** características detalhadas do trabalho — tipo de ocupação, porte do empregador, formas de remuneração habitual e efetiva, home office, afastamento, busca por emprego.

**Variáveis-chave selecionadas:**

| Coluna | Tipo | Descrição |
|---|---|---|
| `trabalho_unico_principal_semana` | string | Tipo de vínculo: `"Empregado do setor privado"`, `"Conta própria"`, etc. |
| `trabalho_area` | string | Área de atuação |
| `carteira_trabalho_assinada_funcionario_publico_estatutario` | string | `"Sim"`, `"Não"` |
| `trabalho_unico_principal_semana_contrato_trabalho_suspenso` | string | `"Sim"`, `"Não"` (contrato suspenso na pandemia) |
| `semana_passada_trabalho_remoto_home_office_teletrabalho` | string | `"Sim"`, `"Não"` |
| `semana_passada_gostaria_ter_trabalhado_horas_fato` | string | `"Sim"`, `"Não"` (subemprego por horas) |
| `valor_dinheiro` | string | Renda habitual em dinheiro (código de faixa) |
| `recebia_retirava_efetivamente_dinheiro` | string | Indicador de recebimento efetivo |
| `quanto_tempo_afastado_trabalho` | string | Tempo de afastamento |
| `semana_passada_quantos_empregados_trabalhavam_negocio` | string | Faixa de porte do empregador |
| `principal_motivo_nao_ter_procurado_trabalho_semana_passada` | string | Motivo de desalento |
| `embora_nao_tenha_procurado_trabalho_gostaria_ter_trabalhado` | string | `"Sim"`, `"Não"` (força de trabalho potencial) |

**Perguntas que responde:**
- Qual era a composição do mercado de trabalho durante a pandemia?
- Quem teve contrato suspenso?
- Quem estava em home office e quem não conseguiu?
- Qual o porte dos empregadores de quem manteve emprego?
- Quem estava em subemprego por insuficiência de horas?

---

## Como usar as bases

### Leitura no PySpark

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('analise').getOrCreate()

# Ler um mês específico
df_saude = spark.read.parquet('output/pnad_covid/base_saude').filter("MES_REF = '2020-11'")

# Ler série completa (todos os meses processados)
df_saude_serie = spark.read.parquet('output/pnad_covid/base_saude')
```

### Leitura no Pandas

```python
import pandas as pd

# Ler Parquet com pandas (requer pyarrow ou fastparquet)
df_saude = pd.read_parquet('output/pnad_covid/base_saude/MES_REF=2020-11/')
```

### Exemplo: % da população com sintomas gripais por região e mês

```python
from pyspark.sql import functions as F

df_saude = spark.read.parquet('output/pnad_covid/base_saude')
df_loc   = spark.read.parquet('output/pnad_covid/dim_localizacao')

chave = ['uf', 'id_domicilio', 'id_morador', 'mes_entrevista', 'MES_REF']

resultado = (df_saude
    .join(df_loc, on=chave, how='left')
    .groupBy('MES_REF', 'regiao')
    .agg(
        (F.sum(F.col('ind_teve_sintoma_gripal') * F.col('peso_amostral')) /
         F.sum('peso_amostral') * 100).alias('pct_sintoma')
    )
    .orderBy('MES_REF', 'regiao'))

resultado.show()
```

### Exemplo: distribuição de situação de trabalho por faixa etária

```python
df_comp  = spark.read.parquet('output/pnad_covid/base_comportamento')
df_perfil = spark.read.parquet('output/pnad_covid/dim_perfil')

chave = ['uf', 'id_domicilio', 'id_morador', 'mes_entrevista', 'MES_REF']

(df_comp
    .join(df_perfil.select(chave + ['faixa_etaria']), on=chave, how='left')
    .filter("MES_REF = '2020-11'")
    .groupBy('faixa_etaria', 'situacao_mercado_trabalho')
    .agg(F.sum('peso_amostral').alias('populacao_estimada'))
    .orderBy('faixa_etaria', 'situacao_mercado_trabalho')
    .show(truncate=False))
```

---

## Como adicionar um novo mês

1. Coloque o arquivo CSV do novo mês em `raw/`
2. Altere o `CONFIG` no início do notebook:

```python
CONFIG = {
    'caminho_microdados':  'raw/PNAD_COVID_102020.csv',
    'mes_referencia':      '2020-10',
    # demais parâmetros permanecem iguais
}
```

3. Execute o notebook. O pipeline sobrescreve **apenas a partição do mês configurado** (`partitionBy('MES_REF')` com `dynamic` overwrite mode) — os meses anteriores não são afetados.

---

## Caveats e limitações conhecidas

| Item | Detalhe |
|---|---|
| **Peso amostral** | Obrigatório em toda agregação. Ver seção "Regra obrigatória" acima. |
| **`area_urbana_rural`** | No dicionário atual, A007 mapeia questão escolar, não área geográfica. Usar `situacao_domicilio` para urbano/rural. |
| **`renda_habitual_trabalho_principal`** | Valor fixo = 1 (indicador de participação, não renda em R$). Usar `renda_efetiva_trabalho_principal` para valores reais, e `valor_dinheiro` (BASE_TRABALHO) para faixas de renda habitual. |
| **Valores `null`** | Representam "Não aplicável" — pessoa não se enquadra naquele quesito (ex.: menor de 14 anos nas questões de trabalho). Diferenciar de "Não sabe" ou "Ignorado", que aparecem como texto. |
| **Variáveis de novembro** | `escola_escola_faculdade_frequenta_publica_privada`, `aulas_presenciais` e `nao_realizou_atividades_disponibilizadas_semana_passada` existem apenas no mês 11/2020. Aparecem como `null` em meses anteriores se os dados forem empilhados. |
| **Encoding** | Os CSVs originais usam `latin1`. O pipeline converte tudo para UTF-8 no Parquet de saída. |

---

## Dicionários externos

Os dois arquivos em `raw/` são a única fonte de metadados do pipeline. Nenhuma transformação é hardcoded no notebook.

### `dicionario_variaveis.csv`
Uma linha por variável (148 linhas). Controla: renomeação de coluna, tipo de dado e bloco temático.

### `dicionario_categorias.csv`
Uma linha por par (variável × valor) — 639 entradas. Controla a tradução de cada código numérico para seu rótulo textual. Variáveis contínuas (renda, horas) não têm entradas com valores enumerados e não são traduzidas.

Para consultar o significado de qualquer coluna:

```python
import pandas as pd
dv = pd.read_csv('raw/dicionario_variaveis.csv')
dc = pd.read_csv('raw/dicionario_categorias.csv')

# O que é a coluna 'motivo_afastamento'?
dv[dv['nome_semantico'] == 'motivo_afastamento'][['codigo_variavel','descricao_variavel','bloco']].to_string()

# Quais são os valores possíveis?
dc[dc['nome_semantico'] == 'motivo_afastamento'][['valor','descricao_valor']].to_string()
```

---

## Dependências

```bash
pip install pyspark pandas pyarrow openpyxl xlrd setuptools
```

> Python 3.12 removeu `distutils` da stdlib. O `setuptools` fornece a shim necessária para o PySpark 3.5.x.
