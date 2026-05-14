# Contexto — Análise Refinada PNAD COVID-19

**Notebook:** `code/refined_analysis_pnad.ipynb`
**Data de criação:** Maio/2026
**Período dos dados:** Novembro de 2020 (único mês disponível nas bases processadas)

---

## Objetivo

Responder 3 perguntas temáticas utilizando no máximo 20 variáveis selecionadas das bases do Star Schema gerado pelo pipeline `raw_to_trusted.ipynb`, e derivar recomendações práticas para hospitais em caso de novo surto de COVID-19.

| Tema | Pergunta respondida |
|---|---|
| 1 | Caracterização dos sintomas clínicos da população |
| 2 | Comportamento da população na época da COVID-19 |
| 3 | Características econômicas da sociedade |

---

## Organização das Bases (Star Schema)

| Tabela | Tipo | Linhas | Colunas | Conteúdo |
|---|---|---|---|---|
| `dim_tempo` | Dimensão | 1 | 4 | Mês, entrevista, fase da pandemia |
| `dim_perfil` | Dimensão | 381.438 | 22 | Sexo, cor/raça, escolaridade, plano de saúde |
| `dim_localizacao` | Dimensão | 381.438 | 8 | UF, região, capital, RM |
| `dim_dicionario` | Metadado | 148 | 8 | Linhagem de todas as variáveis |
| `base_saude` | Temática | 381.438 | 53 | Sintomas, comorbidades, testagem |
| `base_comportamento` | Temática | 381.438 | 29 | Máscara, atendimento, situação laboral |
| `base_economico` | Temática | 381.438 | 29 | Renda, auxílios, proteção social |
| `base_trabalho` | Temática | 381.438 | 48 | Características detalhadas do trabalho |

**Chave de JOIN:** `uf + id_domicilio + id_morador + mes_entrevista`

**Regra obrigatória:** Todas as agregações usam `peso_amostral` como ponderador. A soma dos pesos representa ~213 milhões de pessoas (população brasileira representada).

---

## 20 Variáveis Selecionadas

### Tema 1 — Sintomas Clínicos (`base_saude`)

| # | Coluna real no Parquet | Descrição | Valores |
|---|---|---|---|
| 1 | `ind_teve_sintoma_gripal` | Indicador derivado de síndrome gripal | 0 / 1 |
| 2 | `sintoma_febre` | Teve febre | Sim / Não / Ignorado |
| 3 | `sintoma_tosse` | Teve tosse | Sim / Não / Ignorado |
| 4 | `sintoma_dificuldade_respirar` | Teve dificuldade para respirar | Sim / Não / Ignorado |
| 5 | `sintoma_perda_olfato_paladar` | Teve perda de olfato ou paladar | Sim / Não / Ignorado |
| 6 | `qtd_sintomas_relatados` | Quantidade de sintomas relatados | 0–5 (inteiro) |
| 7 | `resultado_teste_positivo` | Resultado do teste COVID | Positivo / Negativo / Inconclusivo / ... |
| 8 | `medico_deu_diagnostico_diabetes` | Diagnóstico médico de diabetes | Sim / Não / Ignorado |
| 9 | `medico_deu_diagnostico_hipertensao` | Diagnóstico médico de hipertensão | Sim / Não / Ignorado |
| 10 | `medico_deu_diagnostico_doencas_coracao_infarto_angina` | Diagnóstico médico cardíaco | Sim / Não / Ignorado |

### Tema 2 — Comportamento (`base_comportamento`)

| # | Coluna real no Parquet | Descrição | Valores |
|---|---|---|---|
| 11 | `usa_mascara` | Usa máscara | Sim / Não / Ignorado |
| 12 | `buscou_atendimento_saude` | Buscou atendimento de saúde | Sim / Não / Ignorado |
| 13 | `situacao_mercado_trabalho` | Situação laboral derivada | Ocupado/Desocupado/Afastado/Fora |
| 14 | `fez_home_office` | Fez home office | Sim / Não / Não remunerado |
| 15 | `trabalhou_na_semana` | Trabalhou na semana de referência | Sim / Não |

### Tema 3 — Econômico (`base_economico`)

| # | Coluna real no Parquet | Descrição | Valores |
|---|---|---|---|
| 16 | `recebeu_auxilio_emergencial` | Recebeu auxílio emergencial | Sim / Não |
| 17 | `valor_auxilio_emergencial` | Valor do auxílio emergencial recebido | Float (R$) |
| 18 | `renda_efetiva_trabalho_principal` | Renda efetivamente recebida no trabalho principal | Float (R$) |
| 19 | `recebeu_bolsa_familia` | Recebeu Bolsa Família | Sim / Não |
| 20 | `recebeu_seguro_desemprego` | Recebeu seguro desemprego | Sim / Não |

---

## Decisões Técnicas

| Decisão | Escolha | Motivo |
|---|---|---|
| Biblioteca de processamento | PySpark | Consistência com pipeline existente (`raw_to_trusted.ipynb`) |
| Visualização | Matplotlib + Seaborn | Gráficos estáticos de alta qualidade, sem dependências extras |
| Spec de saída | CSV via `.toPandas().to_csv()` | Portabilidade e legibilidade |

---

## Limitações Conhecidas

1. **Período único:** Apenas novembro/2020 foi processado. Análises temporais não são possíveis sem reprocessar os meses anteriores (set e out/2020 estão nos arquivos raw).
2. **`tem_carteira_assinada` mapeada como ocupação:** A coluna na `base_economico` foi mapeada para descrição de ocupação/profissão, não para indicador de emprego formal. Substituída por `recebeu_seguro_desemprego`.
3. **Viés de respondente:** A PNAD COVID-19 é baseada em autodeclaração por telefone, podendo sub-representar populações sem acesso a telefone fixo ou celular.
4. **Comorbidades em < 14 anos:** O questionário de comorbidades é aplicado a moradores de 14 anos ou mais. Para a faixa `00-17` o denominador inclui crianças sem aplicação da pergunta, o que enviesa levemente a taxa para baixo (e, em alguns casos com respostas dadas por pais, para cima).

> **Bug histórico corrigido (2026-05-11):** A coluna `idade` (A002) e outras variáveis contínuas (`ano_nascimento`, `horas_habituais_trabalhadas`, `horas_efetivas_trabalhadas`, `tempo_afastado_*`) eram nulas na trusted zone porque o ETL tratava entradas-domínio do `dicionario_categorias.csv` (ex.: `valor='000 a 130'`) como códigos de categoria. Fix aplicado em `raw_to_trusted.ipynb` e `ETL-PNAD_COVID-CSV.py` (Glue): `cat_maps` agora pula entradas cujo `valor` não cast como int.

---

## Outputs Gerados

| Arquivo | Conteúdo |
|---|---|
| `output/specs/spec_sintomas_clinicos.csv` | Prevalência ponderada de sintomas, comorbidades e testagem |
| `output/specs/spec_comportamento.csv` | % ponderada de comportamentos e situação laboral |
| `output/specs/spec_economico.csv` | Indicadores econômicos, benefícios e renda por grupo |
| `output/specs/fig_01_*.png` a `fig_15_*.png` | Gráficos de todas as análises |
