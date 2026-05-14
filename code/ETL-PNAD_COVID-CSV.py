import os
import sys
import pandas as pd
from itertools import chain as ichain
from functools import reduce

from awsglue.utils import getResolvedOptions

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType

# =========================================================
# ARGUMENTOS GLUE
# =========================================================

args = getResolvedOptions(sys.argv, [
    'RAW_BUCKET_PATH',
    'OUTPUT_S3_PATH',
    'DATABASE_NAME',
])

RAW_BUCKET = args['RAW_BUCKET_PATH']
OUTPUT_PATH = args['OUTPUT_S3_PATH']
DATABASE_NAME = args['DATABASE_NAME']

# =========================================================
# SPARK SESSION
# =========================================================

spark = (
    SparkSession.builder
    .appName('PNAD_COVID19_raw_to_trusted')
    .config('spark.sql.sources.partitionOverwriteMode', 'dynamic')
    .enableHiveSupport()
    .getOrCreate()
)

spark.sparkContext.setLogLevel('ERROR')

# =========================================================
# CRIA DATABASE NO GLUE DATA CATALOG / ATHENA
# =========================================================

spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")
spark.sql(f"USE {DATABASE_NAME}")

# =========================================================
# CONFIG
# =========================================================

CONFIG = {
    'caminho_microdados':   f'{RAW_BUCKET}microdados/PNAD_COVID_*.csv',
    'caminho_variaveis':    f'{RAW_BUCKET}dicionario_variaveis/dicionario_variaveis.csv',
    'caminho_categorias':   f'{RAW_BUCKET}dicionario_categorias/dicionario_categorias.csv',
    'output_base':          OUTPUT_PATH,
    'encoding_microdados':  'latin1',
    'separador_microdados': ',',
}

print('======================================')
print('INICIANDO PIPELINE PNAD COVID')
print('RAW_BUCKET:', RAW_BUCKET)
print('OUTPUT_PATH:', OUTPUT_PATH)
print('DATABASE_NAME:', DATABASE_NAME)
print('MICRODADOS:', CONFIG['caminho_microdados'])
print('======================================')

# =========================================================
# LEITURA DOS DICIONARIOS
# =========================================================

dv_pd = pd.read_csv(
    CONFIG['caminho_variaveis'],
    dtype=str,
    encoding='utf-8'
)

dc_pd = pd.read_csv(
    CONFIG['caminho_categorias'],
    dtype=str,
    encoding='utf-8'
)

# =========================================================
# MAPAS
# =========================================================

mapa_renomear = dict(
    zip(dv_pd['codigo_variavel'], dv_pd['nome_semantico'])
)

mapa_tipos = dict(
    zip(dv_pd['nome_semantico'], dv_pd['tipo'])
)

mapa_dominio = dict(
    zip(dv_pd['nome_semantico'], dv_pd['dominio'].fillna(''))
)

mapa_blocos = (
    dv_pd
    .groupby('bloco')['nome_semantico']
    .apply(list)
    .to_dict()
)

# =========================================================
# MAPA DE CATEGORIAS
# =========================================================

cat_maps = {}

for _, row in dc_pd.iterrows():

    valor = row['valor']

    if pd.isna(valor) or str(valor).strip() == '':
        continue

    try:
        chave = int(float(valor))
    except (ValueError, TypeError):
        # valor não-numérico = descrição de domínio (ex.: '000 a 130', 'Ano'),
        # não código de categoria → variável é contínua, ignora a entrada
        continue

    nome = mapa_renomear.get(
        row['codigo_variavel'],
        row['codigo_variavel']
    )

    cat_maps.setdefault(nome, {})[chave] = row['descricao_valor']

# =========================================================
# CHAVES
# =========================================================

CHAVE_JOIN = [
    'uf',
    'id_domicilio',
    'id_morador',
    'mes_entrevista'
]

BASE_COLS = (
    mapa_blocos.get('id', []) +
    mapa_blocos.get('peso', []) +
    ['MES_REF']
)

print(f'Variáveis: {len(dv_pd)}')
print(f'Categorias: {len(dc_pd)}')
print(f'Blocos: {sorted(mapa_blocos)}')

# =========================================================
# LEITURA MICRODADOS
# =========================================================
# As colunas dos CSVs estão em POSIÇÕES diferentes entre meses (Nov/2020 tem 3
# colunas extras inseridas no meio — A006A/A006B/A007A — deslocando todas as
# seguintes). Ler com glob + header=true desalinha os dados porque Spark usa o
# header de UM arquivo e mapeia os outros posicionalmente. Solução: ler cada
# CSV individualmente preservando o próprio header e unir via unionByName
# com allowMissingColumns=True. MES_REF é extraído do nome do arquivo
# (mantém o critério de não hardcoding por mês).

import re as _re
from functools import reduce as _reduce

# Lista os arquivos do glob via Hadoop FileSystem (compatível com S3 no Glue).
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
fs_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(
    CONFIG['caminho_microdados']
)
fs = fs_path.getFileSystem(hadoop_conf)
statuses = fs.globStatus(fs_path)
arquivos = sorted([s.getPath().toString() for s in statuses])

print(f'Arquivos detectados: {len(arquivos)}')
for a in arquivos:
    print(f'  {a}')

dfs = []
for caminho in arquivos:
    nome_arq = caminho.rsplit('/', 1)[-1]
    m = _re.search(r'PNAD_COVID_(\d{2})(\d{4})\.csv', nome_arq)
    if not m:
        continue
    mes_ref = f'{m.group(2)}-{m.group(1)}'   # MMAAAA → AAAA-MM
    df_m = (
        spark.read
        .option('header', 'true')
        .option('sep', CONFIG['separador_microdados'])
        .option('encoding', CONFIG['encoding_microdados'])
        .option('inferSchema', 'false')
        .csv(caminho)
        .withColumn('MES_REF', F.lit(mes_ref))
    )
    dfs.append(df_m)

df_raw = _reduce(
    lambda a, b: a.unionByName(b, allowMissingColumns=True),
    dfs
)

# =========================================================
# RENOMEAR COLUNAS
# =========================================================

codigos_validos = set(df_raw.columns) & set(mapa_renomear)

ausentes = set(mapa_renomear) - set(df_raw.columns)

if ausentes:
    print(f'Variáveis ausentes: {len(ausentes)}')

df_renamed = df_raw.select(
    [F.col(c).alias(mapa_renomear[c]) for c in codigos_validos] +
    [F.col('MES_REF')]
)

# =========================================================
# TIPAGEM
# =========================================================

def aplicar_tipos(df, mapa):

    for col, tipo in mapa.items():

        if col not in df.columns:
            continue

        if tipo == 'integer':
            df = df.withColumn(
                col,
                F.col(col).cast(IntegerType())
            )

        elif tipo == 'double':
            df = df.withColumn(
                col,
                F.col(col).cast(DoubleType())
            )

    return df

df_typed = aplicar_tipos(df_renamed, mapa_tipos)

# =========================================================
# TRADUÇÃO CATEGORIAS
# =========================================================

def traduzir_categorias(df):

    for nome, mp in cat_maps.items():

        if nome not in df.columns:
            continue

        if mapa_tipos.get(nome) == 'double':
            continue

        if mapa_dominio.get(nome, '').strip():
            continue

        flat = [F.lit(x) for x in ichain(*mp.items())]

        if not flat:
            continue

        mapa_expr = F.create_map(flat)

        df = df.withColumn(
            nome,
            mapa_expr[F.col(nome)]
        )

    return df

df_labeled = traduzir_categorias(df_typed)

print(f'Linhas carregadas: {df_labeled.count():,}')

# =========================================================
# FUNÇÃO BASE
# =========================================================

def montar_base(bloco, extra_cols=None):

    cols = mapa_blocos.get(bloco, [])

    all_cols = BASE_COLS + cols + (extra_cols or [])

    disponiveis = [
        c for c in all_cols
        if c in df_labeled.columns
    ]

    seen = set()

    return df_labeled.select([
        c for c in disponiveis
        if not (c in seen or seen.add(c))
    ])

# =========================================================
# DIM TEMPO
# =========================================================

dim_tempo = (
    df_labeled
    .select(
        'mes_entrevista',
        'num_entrevista',
        'MES_REF'
    )
    .distinct()
    .withColumn(
        'fase_pandemia',
        F.when(
            F.col('mes_entrevista').isin(5, 6),
            'Inicio da Pandemia'
        )
        .when(
            F.col('mes_entrevista').isin(7, 8, 9),
            'Pico da Primeira Onda'
        )
        .otherwise('Desaceleracao')
    )
)

# =========================================================
# DIM PERFIL
# =========================================================

perfil_cols = [
    c for c in mapa_blocos.get('perfil', [])
    if c in df_labeled.columns
]

dim_perfil = (
    df_labeled
    .select(CHAVE_JOIN + ['MES_REF'] + perfil_cols)
    .withColumn(
        'faixa_etaria',
        F.when(F.col('idade') < 18, '00-17')
        .when(F.col('idade') < 30, '18-29')
        .when(F.col('idade') < 45, '30-44')
        .when(F.col('idade') < 60, '45-59')
        .when(F.col('idade') < 75, '60-74')
        .otherwise('75+')
    )
)

# =========================================================
# DIM LOCALIZACAO
# =========================================================

loc_cols = [
    c for c in [
        'situacao_domicilio',
        'mora_na_capital',
        'mora_em_regiao_metropolitana'
    ]
    if c in df_labeled.columns
]

norte = [
    'Rondônia', 'Acre', 'Amazonas',
    'Roraima', 'Pará', 'Amapá', 'Tocantins'
]

nordeste = [
    'Maranhão', 'Piauí', 'Ceará',
    'Rio Grande do Norte', 'Paraíba',
    'Pernambuco', 'Alagoas',
    'Sergipe', 'Bahia'
]

sudeste = [
    'Minas Gerais',
    'Espírito Santo',
    'Rio de Janeiro',
    'São Paulo'
]

sul = [
    'Paraná',
    'Santa Catarina',
    'Rio Grande do Sul'
]

centro_oeste = [
    'Mato Grosso do Sul',
    'Mato Grosso',
    'Goiás',
    'Distrito Federal'
]

dim_localizacao = (
    df_labeled
    .select(CHAVE_JOIN + ['MES_REF'] + loc_cols)
    .withColumn(
        'regiao',
        F.when(F.col('uf').isin(norte), 'Norte')
        .when(F.col('uf').isin(nordeste), 'Nordeste')
        .when(F.col('uf').isin(sudeste), 'Sudeste')
        .when(F.col('uf').isin(sul), 'Sul')
        .when(F.col('uf').isin(centro_oeste), 'Centro-Oeste')
        .otherwise(None)
    )
)

# =========================================================
# DIM DICIONARIO
# =========================================================

dim_dicionario = spark.createDataFrame(
    dv_pd[
        [
            'codigo_variavel',
            'nome_semantico',
            'descricao_variavel',
            'bloco',
            'tipo',
            'dominio',
            'parte',
            'meses'
        ]
    ]
)

# =========================================================
# BASE SAUDE
# =========================================================

base_saude = montar_base('saude')

sintomas = [
    c for c in [
        'sintoma_febre',
        'sintoma_tosse',
        'sintoma_dificuldade_respirar',
        'sintoma_fadiga',
        'sintoma_perda_olfato_paladar'
    ]
    if c in base_saude.columns
]

cond_sintoma = F.lit(False)

for s in sintomas:
    cond_sintoma = cond_sintoma | (F.col(s) == 'Sim')

qtd_expr = (
    sum(
        F.when(F.col(s) == 'Sim', 1).otherwise(0)
        for s in sintomas
    )
    if sintomas else F.lit(0)
)

base_saude = (
    base_saude
    .withColumn(
        'ind_teve_sintoma_gripal',
        F.when(cond_sintoma, 1).otherwise(0)
    )
    .withColumn(
        'qtd_sintomas_relatados',
        qtd_expr
    )
)

# =========================================================
# BASE COMPORTAMENTO
# =========================================================

base_comportamento = montar_base('comportamento')

base_comportamento = (
    base_comportamento
    .withColumn(
        'situacao_mercado_trabalho',
        F.when(
            F.col('trabalhou_na_semana') == 'Sim',
            'Ocupado - trabalhou'
        )
        .when(
            (
                (F.col('trabalhou_na_semana') == 'Não') &
                (F.col('estava_afastado_com_vinculo') == 'Sim')
            ),
            'Ocupado - afastado'
        )
        .when(
            (
                (F.col('trabalhou_na_semana') == 'Não') &
                (F.col('estava_afastado_com_vinculo') == 'Não') &
                (F.col('procurou_trabalho_semana') == 'Sim')
            ),
            'Desocupado'
        )
        .when(
            (
                (F.col('trabalhou_na_semana') == 'Não') &
                (F.col('estava_afastado_com_vinculo') == 'Não') &
                (F.col('procurou_trabalho_semana') == 'Não')
            ),
            'Fora da forca de trabalho'
        )
        .otherwise('Nao se aplica')
    )
)

# =========================================================
# BASE ECONOMICO
# =========================================================

base_economico = montar_base('economico')

if 'renda_efetiva_trabalho_principal' in base_economico.columns:

    base_economico = (
        base_economico
        .withColumn(
            'ind_tem_renda_efetiva',
            F.when(
                F.col('renda_efetiva_trabalho_principal') > 0,
                1
            ).otherwise(0)
        )
    )

# =========================================================
# BASE TRABALHO
# =========================================================

base_trabalho = montar_base('trabalho')

# =========================================================
# OUTPUT
# =========================================================

OUTPUT = CONFIG['output_base']

tabelas = [
    ('DIM_TEMPO', dim_tempo),
    ('DIM_PERFIL', dim_perfil),
    ('DIM_LOCALIZACAO', dim_localizacao),
    ('DIM_DICIONARIO', dim_dicionario),
    ('BASE_SAUDE', base_saude),
    ('BASE_COMPORTAMENTO', base_comportamento),
    ('BASE_ECONOMICO', base_economico),
    ('BASE_TRABALHO', base_trabalho),
]

# =========================================================
# ESCRITA S3 + CRIAÇÃO DAS TABELAS NO GLUE CATALOG
# =========================================================

for nome, df in tabelas:

    tabela = nome.lower()
    path = f'{OUTPUT}/{tabela}'

    print('======================================')
    print(f'Gravando tabela: {DATABASE_NAME}.{tabela}')
    print(f'Path: {path}')
    print('======================================')

    spark.sql(f"DROP TABLE IF EXISTS {DATABASE_NAME}.{tabela}")

    if nome == 'DIM_DICIONARIO':

        (
            df.write
            .mode('overwrite')
            .format('parquet')
            .option('path', path)
            .saveAsTable(f'{DATABASE_NAME}.{tabela}')
        )

    else:

        (
            df.write
            .mode('overwrite')
            .format('parquet')
            .option('path', path)
            .partitionBy('MES_REF')
            .saveAsTable(f'{DATABASE_NAME}.{tabela}')
        )

# =========================================================
# REPARA PARTIÇÕES NO ATHENA / GLUE CATALOG
# =========================================================

for nome, df in tabelas:

    tabela = nome.lower()

    if nome != 'DIM_DICIONARIO':
        print(f'Reparando partições: {DATABASE_NAME}.{tabela}')
        spark.sql(f"MSCK REPAIR TABLE {DATABASE_NAME}.{tabela}")

# =========================================================
# VALIDAÇÃO
# =========================================================

print('\n====================')
print('VALIDAÇÃO')
print('====================')

for nome, _ in tabelas:

    tabela = nome.lower()

    total = spark.sql(
        f"SELECT COUNT(*) AS total FROM {DATABASE_NAME}.{tabela}"
    ).collect()[0]['total']

    print(f'{DATABASE_NAME}.{tabela:<25} {total:>10,} linhas')

# =========================================================
# FINAL
# =========================================================

spark.stop()

print('======================================')
print('PIPELINE FINALIZADO COM SUCESSO')
print('BANCO E TABELAS CRIADOS NO ATHENA')
print('======================================')