# Insights — Análise Refinada PNAD COVID-19

**Fonte:** PNAD COVID-19 — IBGE — Novembro de 2020
**Notebook:** `code/refined_analysis_pnad.ipynb`
**Representatividade:** ~213 milhões de pessoas (via `peso_amostral`)

---

## O que foi feito

Criado notebook analítico completo (`refined_analysis_pnad.ipynb`) com **28 células**, organizadas em 5 seções:

1. **Organização do banco:** documentação do Star Schema, chave de join, regra do peso amostral
2. **Seleção das 20 variáveis:** tabela justificada por tema
3. **Sintomas Clínicos:** 5 análises com gráficos
4. **Comportamento da população:** 5 análises com gráficos
5. **Características econômicas:** 5 análises com gráficos
6. **Recomendações ao hospital:** 5 eixos de ação derivados dos dados

**Outputs gerados:**
- `output/specs/spec_sintomas_clinicos.csv`
- `output/specs/spec_comportamento.csv`
- `output/specs/spec_economico.csv`
- 15 gráficos PNG (`fig_01_*.png` a `fig_15_*.png`)

---

## Principais Achados por Tema

### 1. Sintomas Clínicos

| Indicador | Valor ponderado |
|---|---|
| Síndrome gripal (qualquer sintoma) | **2,24%** da população |
| Tosse | 1,43% |
| Febre | 0,84% |
| Fadiga | 0,72% |
| Dificuldade respirar | 0,43% |
| Perda de olfato/paladar | 0,38% |
| Realizaram teste COVID | **13,51%** |
| Positivos entre testados | **7,15%** |
| Positivos na população geral | 0,97% |

**Comorbidades na população:**

| Comorbidade | Prevalência ponderada |
|---|---|
| Hipertensão | **13,32%** |
| Diabetes | 5,26% |
| Doenças do coração | 2,59% |

**Severidade:** 97,8% da população não relatou nenhum sintoma. Entre os sintomáticos, a maioria relatou apenas 1 sintoma (1,35%). Casos com 3+ sintomas (potencialmente mais graves) representam ~0,4%.

**Síndrome gripal por faixa etária:**

| Faixa | Taxa ponderada |
|---|---|
| 00-17 | 1,85% |
| 18-29 | 2,18% |
| 30-44 | 2,54% |
| 45-59 | 2,61% |
| 60-74 | 2,66% |
| 75+ | 2,65% |

Tendência crescente com a idade, plateau a partir dos 45 anos — adultos com mais comorbidades possivelmente reportam sintomas com mais frequência.

**Comorbidades por faixa etária (prevalência ponderada):**

| Faixa | Hipertensão | Diabetes | Doenças Cardíacas |
|---|---|---|---|
| 00-17 | 4,6% | 2,0% | 1,1% |
| 18-29 | 11,9% | 4,8% | 2,4% |
| 30-44 | 19,4% | 7,6% | 3,7% |
| 45-59 | 21,1% | 8,2% | 4,1% |
| 60-74 | 22,0% | 8,6% | 4,3% |
| 75+ | 21,7% | 8,5% | 4,2% |

> Nota: para `00-17` o denominador inclui crianças, mas a pergunta só é aplicada a partir dos 14 anos — as taxas devem ser lidas como prevalência do grupo amostrado dentro da faixa.

**Insight-chave:** A baixa taxa de testagem (13,5%) apesar de 2,24% de síndrome gripal indica que boa parte dos casos não foi confirmada laboratorialmente. A positividade de 7,15% entre testados, em novembro/2020, sugere transmissão ativa relevante mesmo em fase de "desaceleração". O cruzamento por faixa etária revela que **a partir dos 45 anos, 1 em cada 5 brasileiros é hipertenso** — população de altíssima prioridade para triagem em surto.

---

### 2. Comportamento da População

**Situação no mercado de trabalho (excluindo "Não se aplica"):**

| Situação | % ponderada |
|---|---|
| Ocupado — trabalhou presencialmente | **48,1%** |
| Fora da força de trabalho | **44,7%** |
| Ocupado — afastado com vínculo | 4,0% |
| Desocupado | 3,2% |

**Isolamento laboral:**
- 38,81% da população trabalharam na semana de referência
- Apenas 0,83% do total estava em home office — dado que reflete que home office era opção restrita a trabalhadores de escritório/serviços (minoria da força de trabalho formal)

**Uso de máscara:** A pergunta foi condicional — respondida apenas por quem buscou atendimento de saúde, portanto não representa a população geral.

**Busca por atendimento:** 1,10% da população geral buscou atendimento de saúde na semana de referência. Entre os sintomáticos essa taxa é significativamente superior (calculada no notebook).

**Insight-chave:** Quase metade da população economicamente ativa continuou trabalhando presencialmente em novembro/2020. O home office foi restrito a uma parcela muito pequena. Isso representa um risco epidemiológico estrutural que o hospital precisa considerar: a maioria dos potenciais infectados chegou ao serviço de saúde vinda de ambientes de trabalho presencial.

---

### 3. Características Econômicas

**Proteção social (% que recebeu no mês):**

| Benefício | Cobertura |
|---|---|
| Auxílio Emergencial | **47,23%** |
| Aposentadoria/Pensão | 31,36% |
| Bolsa Família | 8,97% |
| Seguro Desemprego | 3,17% |

**Sobreposição de benefícios:**

| Grupo | % da população |
|---|---|
| Nenhum benefício | 51,1% |
| Apenas Auxílio Emergencial | 39,9% |
| AE + Bolsa Família | 7,3% |
| Apenas Bolsa Família | 1,6% |

**Renda:**
- **61,55%** da população não teve renda efetiva do trabalho no mês (inclui crianças, aposentados e desempregados)
- Mediana da renda efetiva (entre quem trabalhou): **R$ 1.450**
- Média da renda efetiva: **R$ 2.228** (puxada pela cauda superior)
- Valor médio do Auxílio Emergencial recebido: **R$ 575** (abaixo do valor oficial de R$ 600, indicando parcelas reduzidas/proporcionais)

**Insight-chave:** Em novembro/2020, o Auxílio Emergencial era o principal sustento de quase metade da população. Com a mediana de renda de R$ 1.450 e o auxílio médio de R$ 575, fica evidente a vulnerabilidade financeira: qualquer internação prolongada representa impacto econômico severo na renda familiar. Hospitais precisam ter estrutura de assistência social para este perfil de paciente.

---

## Limitações Metodológicas

1. **Período único (novembro/2020):** Não é possível analisar evolução temporal sem reprocessar os meses anteriores.
2. **Variáveis condicionais:** `usa_mascara`, `buscou_atendimento_saude` e algumas variáveis de comportamento eram perguntadas apenas a subgrupos (sintomáticos que buscaram atendimento), e não à população geral. Os percentuais calculados sobre o total devem ser interpretados como "prevalência no subgrupo" e não como "comportamento geral".
3. **Autodeclaração por telefone:** A pesquisa pode sub-representar populações sem acesso a telefone fixo/celular, que tendem a ser as mais vulneráveis.
4. **`tem_carteira_assinada` mapeada incorretamente:** Contém descrição de ocupação, não indicador de emprego formal. Substituída por `recebeu_seguro_desemprego`.
5. **Comorbidades em `00-17`:** O questionário só é aplicado a partir de 14 anos; o denominador da faixa inclui crianças sem aplicação da pergunta, então a taxa do grupo é uma aproximação por baixo.

> **Bug histórico corrigido (2026-05-11):** Antes desta versão, `idade` e outras variáveis contínuas estavam nulas na trusted zone por um bug no ETL que confundia descrições de domínio (`'000 a 130'`, `'Ano'`) com códigos de categoria. Fix aplicado em [code/raw_to_trusted.ipynb](../code/raw_to_trusted.ipynb) e [code/ETL-PNAD_COVID-CSV.py](../code/ETL-PNAD_COVID-CSV.py).

---

## Recomendações ao Hospital (Resumo Executivo)

### 1. Triagem Clínica
- **Protocolo de triagem rápida** para tosse + febre (sintomas mais prevalentes)
- **Sinalização de alto risco** imediato para pacientes com dificuldade respiratória
- **Rastreio obrigatório de comorbidades** (hipertensão em 13% da população, diabetes em 5%)
- **Pacientes a partir de 45 anos:** 1 em cada 5 é hipertenso (21%), 8% são diabéticos — fluxo prioritário para esta faixa

### 2. Dimensionamento de Capacidade
- Com 2,24% de síndrome gripal e positividade de 7,15%, um hospital deve reservar capacidade proporcional para internações COVID, especialmente em população > 60 anos e hipertensos
- A baixa testagem (13,5%) sugere que muitos casos chegam ao hospital sem diagnóstico prévio — triagem ativa é essencial

### 3. Prevenção e Comunicação
- Campanhas focadas em **trabalhadores presenciais** (48% da PEA): principal grupo de exposição
- Testagem ativa em locais de trabalho reduz transmissão antes da chegada ao hospital
- Home office deve ser incentivado como política de saúde pública nas próximas ondas

### 4. Suporte Socioeconômico
- 61,55% sem renda efetiva e 47% dependendo do Auxílio Emergencial (R$ 575/mês) → necessidade de **assistência social integrada ao atendimento hospitalar**
- Programa de isenção ou parcelamento de despesas para beneficiários de AE e BF
- Parceria com CRAS/CREAS para continuidade de suporte após alta hospitalar

### 5. Ação Territorial Prioritária
- Regiões **Norte e Nordeste**: menor renda, menor acesso ao home office, maior concentração de trabalhadores informais
- Populações rurais com acesso limitado a UBS/pronto-socorro devem ter acesso a unidades móveis de testagem
- Foco em famílias que acumulam AE + BF (dupla vulnerabilidade: baixa renda pré-pandemia + desemprego na pandemia)

---

## Próximos Passos Sugeridos

1. Reprocessar meses de setembro e outubro/2020 para análise de evolução temporal
2. Corrigir o ETL para persistir `idade` e `ano_nascimento` na `dim_perfil`
3. Cruzar com dados do SIVEP-Gripe para validar a taxa de internação derivada dos sintomas
4. Segmentar análise por UF para identificar estados prioritários para intervenção hospitalar
