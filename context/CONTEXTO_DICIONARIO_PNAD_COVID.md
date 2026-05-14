# Contexto — Dicionário consolidado PNAD COVID 2020

Documento para outro chat de Claude usar como referência única ao escrever
código de tradução/análise dos microdados da PNAD COVID 2020 (set/out/nov).
Os 3 dicionários originais (`Dicionario_PNAD_COVID_092020/102020/112020.xls`)
foram consolidados em **uma base única**, com nomes semânticos para usar
como nomes de coluna no DataFrame final.

---

## 1. Arquivos

Microdados (entrada — não modificar):
- `PNAD_COVID_092020.csv` — 145 colunas
- `PNAD_COVID_102020.csv` — 145 colunas
- `PNAD_COVID_112020.csv` — 148 colunas (3 perguntas a mais)

Dicionários consolidados (a usar):
- `dicionario_variaveis.csv` — 1 linha por variável (148 linhas)
- `dicionario_categorias.csv` — 1 linha por par (variável, valor) (639 linhas)
- `dicionario_pnad_covid_consolidado.xlsx` — versão Excel com as duas abas

A chave entre tudo é `codigo_variavel` (nome exato da coluna no CSV de
microdados — ex.: `UF`, `C003`, `V1012`).

---

## 2. Esquema das tabelas

### 2.1 `dicionario_variaveis.csv`

| coluna | descrição |
|---|---|
| `codigo_variavel` | nome da coluna no CSV de microdados (chave) |
| `nome_semantico` | **nome em snake_case para usar no DataFrame** (ex.: `motivo_afastamento`, `principal_motivo_nao_ter_procurado_trabalho_semana_passada`) |
| `descricao_variavel` | pergunta original do questionário |
| `bloco` | agrupamento temático: `id`, `perfil`, `saude`, `comportamento`, `trabalho`, `economico`, `rendimento`, `emprestimos`, `habitacao`, `peso` |
| `tipo` | `integer`, `double` ou `string` |
| `dominio` | descrição do domínio para variáveis numéricas contínuas (ex.: "valor em reais", "01 a 30"). Vazio para variáveis categóricas |
| `tamanho` | tamanho do campo no layout original |
| `quesito` | nº do quesito no questionário |
| `parte` | seção do questionário (texto completo) |
| `meses` | meses em que a variável aparece (ex.: `092020,102020,112020` para variáveis presentes nos 3 meses; `112020` para as que só existem em novembro) |

> **Importante**: das 148 variáveis, **145 estão em todos os 3 meses** com a mesma semântica. Só 3 são exclusivas de novembro/2020: `A006A`, `A006B`, `A007A`. Para a maioria dos casos é seguro tratar a base como **única e unificada**.

### 2.2 `dicionario_categorias.csv`

| coluna | descrição |
|---|---|
| `codigo_variavel` | código da variável (chave) |
| `nome_semantico` | mesmo nome semântico da tabela de variáveis (já joinado para conveniência) |
| `valor` | código numérico que aparece na célula do microdado |
| `descricao_valor` | tradução do código (rótulo legível) |
| `parte` | seção do questionário |
| `meses` | meses em que aquela categoria aparece |

> Variáveis numéricas contínuas (com `dominio` preenchido na tabela de variáveis) **não têm** linhas em categorias com valores enumerados — apenas a entrada de "Não aplicável" (valor vazio) quando aplicável.

---

## 3. Convenções importantes (não ignorar)

1. **Nomes semânticos como nomes de coluna**: ao traduzir o microdado, troque o `codigo_variavel` pelo `nome_semantico`. Ex.: `df['C003']` vira `df['motivo_afastamento']`.

2. **Variáveis numéricas contínuas**: as que têm `dominio` preenchido (ex.: `D0013` = "valor em reais", `A001` = "01 a 30") **não devem ter os valores mapeados via dicionário de categorias**. Use os números diretamente. Para essas, a única "categoria" possível é "Não aplicável" (= célula em branco no microdado).

3. **"Não aplicável"** aparece como `valor` vazio + `descricao_valor = "Não aplicável"`. Corresponde a células em branco/`NaN` no microdado. Trate como missing.

4. **Coluna `meses`**: use só se for fazer análise longitudinal. Para a maioria dos usos pode ignorar — quase todas as variáveis estão nos 3 meses.

5. **Encoding UTF-8** em ambos os CSVs.

6. **Cobertura validada 100%**: todas as colunas dos 3 CSVs de microdados têm entrada em `dicionario_variaveis.csv`.

---

## 4. Como usar (Python / pandas)

```python
import pandas as pd

# ---- Carregar ----
dv = pd.read_csv("dicionario_variaveis.csv")
dc = pd.read_csv("dicionario_categorias.csv")

df = pd.read_csv("PNAD_COVID_112020.csv")  # ou concatenar os 3 com coluna 'mes'

# ---- 1. Renomear colunas para nomes semânticos ----
mapa_colunas = dict(zip(dv["codigo_variavel"], dv["nome_semantico"]))
df = df.rename(columns=mapa_colunas)
# agora df tem colunas como motivo_afastamento, idade, sexo, uf, etc.

# ---- 2. Traduzir valores categóricos ----
# (usar os nomes semânticos como chave porque já renomeamos as colunas)

def construir_mapa_por_codigo(codigo):
    sub = dc[(dc["codigo_variavel"] == codigo) & dc["valor"].notna()]
    if len(sub) == 0:
        return {}
    # casa o tipo do valor com o que vem no CSV (geralmente int)
    try:
        chaves = sub["valor"].astype(int)
    except Exception:
        chaves = sub["valor"]
    return dict(zip(chaves, sub["descricao_valor"]))

# variáveis que têm categorias enumeradas (são as que precisam de tradução)
codigos_categoricos = (dc[dc["valor"].notna()]
                       .groupby("codigo_variavel").size().index.tolist())

for cod in codigos_categoricos:
    nome = mapa_colunas.get(cod, cod)  # nome no DataFrame após rename
    if nome not in df.columns:
        continue
    mp = construir_mapa_por_codigo(cod)
    if mp:
        df[f"{nome}_label"] = df[nome].map(mp)
        # ou substitua direto, sem manter o código:
        # df[nome] = df[nome].map(mp).fillna(df[nome])

# ---- 3. Filtrar só as variáveis ativas/de interesse por bloco ----
vars_saude = dv[dv["bloco"] == "saude"]["nome_semantico"].tolist()
df_saude = df[vars_saude]

# ---- 4. Para análise longitudinal: usar só vars presentes nos 3 meses ----
vars_comuns = dv[dv["meses"] == "092020,102020,112020"]["nome_semantico"].tolist()
```

### Dicas de tipo

- O CSV do IBGE traz códigos como inteiros ou em branco. No `dicionario_categorias.csv`
  o `valor` foi normalizado para inteiro quando possível. Se `.map` não casar, use
  `.astype(str)` dos dois lados.
- Variáveis com `tipo=double` (`dominio="valor em reais"`) são monetárias — converta
  com `pd.to_numeric(df[col], errors="coerce")` antes de agregar.

---

## 5. Recomendações de modelagem

- **Pipeline reproduzível**: leia os 3 CSVs com uma coluna `mes` e concatene em
  um único DataFrame antes de traduzir. Use `meses` da tabela de variáveis
  para decidir o que fica e o que sai.
- **Duas versões do DataFrame**: uma com códigos originais (joins/agregações)
  e uma com rótulos traduzidos (gráficos/relatórios). O sufixo `_label` na
  coluna traduzida ajuda a manter as duas em paralelo.
- **Missing values**: códigos `9`, `99`, `999` em algumas variáveis significam
  "Ignorado" / "Não sabe" — confira na tabela de categorias antes de tratar
  como valor real.

---

## 6. Resumo numérico

| | set/2020 | out/2020 | nov/2020 | unificado |
|---|---|---|---|---|
| linhas (microdado) | ~388k | ~382k | ~388k | — |
| colunas | 145 | 145 | 148 | — |
| variáveis no dicionário | 145 | 145 | 148 | **148** |
| categorias no dicionário | 635 | 635 | 650 | **639** |

Distribuição por bloco: saude (36), trabalho (32), perfil (17), comportamento (13),
economico (13), id (13), habitacao (10), rendimento (7), emprestimos (5), peso (2).

Distribuição por tipo: integer (138), double (6), string (4).
