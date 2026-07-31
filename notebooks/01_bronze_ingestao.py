# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Ingestão da Camada Bronze
# MAGIC
# MAGIC Este notebook prepara o ambiente e executa a ingestão incremental dos arquivos de origem para tabelas Delta na camada Bronze.
# MAGIC
# MAGIC ## Responsabilidades
# MAGIC
# MAGIC - Validar o ambiente Databricks e Spark;
# MAGIC - Criar schemas auxiliares;
# MAGIC - Criar o volume de arquivos de entrada;
# MAGIC - Configurar caminhos e parâmetros;
# MAGIC - Ler arquivos CSV ou JSON;
# MAGIC - Preservar os dados recebidos;
# MAGIC - Adicionar metadados técnicos de ingestão;
# MAGIC - Persistir os dados em tabelas Delta;
# MAGIC - Permitir reprocessamento com controle por `batch_id`.
# MAGIC
# MAGIC ## Fontes previstas
# MAGIC
# MAGIC - clientes_cdc
# MAGIC - contas_cdc
# MAGIC - cartoes_cdc
# MAGIC - transacoes
# MAGIC - eventos_risco
# MAGIC - estornos

# COMMAND ----------

# Importações utilizadas pela ingestão Bronze.

import uuid

from datetime import datetime
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# COMMAND ----------

# Validação inicial do ambiente Databricks e do Spark.

print("Iniciando validação do ambiente Databricks")

versao_spark = spark.version

print(f"Spark disponível. Versão: {versao_spark}")

spark.range(5).show()

# COMMAND ----------

# Configurações recebidas pelo orquestrador.
dbutils.widgets.text("catalogo", "workspace")
dbutils.widgets.text("schema_bronze", "bronze")
dbutils.widgets.text("schema_prata", "prata")
dbutils.widgets.text("schema_ouro", "ouro")
dbutils.widgets.text("schema_quarentena", "quarentena")
dbutils.widgets.text(
    "schema_observabilidade",
    "observabilidade",
)

CATALOGO = dbutils.widgets.get("catalogo")
SCHEMA_BRONZE = dbutils.widgets.get("schema_bronze")
SCHEMA_PRATA = dbutils.widgets.get("schema_prata")
SCHEMA_OURO = dbutils.widgets.get("schema_ouro")
SCHEMA_QUARENTENA = dbutils.widgets.get("schema_quarentena")
SCHEMA_OBSERVABILIDADE = dbutils.widgets.get(
    "schema_observabilidade"
)
print(f"CATALOGO: {CATALOGO}")
print(f"SCHEMA_BRONZE: {SCHEMA_BRONZE}")
print(f"SCHEMA_PRATA: {SCHEMA_PRATA}")
print(f"SCHEMA_OURO: {SCHEMA_OURO}")
print(f"SCHEMA_QUARENTENA: {SCHEMA_QUARENTENA}")
print(f"SCHEMA_OBSERVABILIDADE: {SCHEMA_OBSERVABILIDADE}")

# COMMAND ----------

# Define o catálogo padrão utilizado pelas próximas operações SQL.

spark.sql(f"USE CATALOG {CATALOGO}")

catalogo_atual = spark.sql("SELECT current_catalog()").collect()[0][0]

print(f"Catálogo atual: {catalogo_atual}")

# COMMAND ----------

# Cria os schemas utilizados pela arquitetura Medallion e pelos componentes
# auxiliares de qualidade e observabilidade.

schemas_projeto = [
    SCHEMA_BRONZE,
    SCHEMA_PRATA,
    SCHEMA_OURO,
    SCHEMA_QUARENTENA,
    SCHEMA_OBSERVABILIDADE,
]

for nome_schema in schemas_projeto:
    spark.sql(
        f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{nome_schema}"
    )

    print(
        f"Schema validado: {CATALOGO}.{nome_schema}"
    )

# COMMAND ----------

# Lista os schemas existentes no catálogo workspace.

spark.sql(
    f"SHOW SCHEMAS IN {CATALOGO}"
).show(truncate=False)

# COMMAND ----------

# Cria o volume gerenciado que receberá os arquivos de entrada.

spark.sql(
    f"""
    CREATE VOLUME IF NOT EXISTS
    {CATALOGO}.{SCHEMA_BRONZE}.{NOME_VOLUME}
    """
)

print(f"Volume validado: {CAMINHO_VOLUME}")

# COMMAND ----------

# Exibe os volumes existentes no schema Bronze.

spark.sql(
    f"SHOW VOLUMES IN {CATALOGO}.{SCHEMA_BRONZE}"
).show(truncate=False)

# COMMAND ----------

# Cria uma pasta separada para cada fonte de dados.

fontes = [
    "clientes",
    "contas",
    "cartoes",
    "transacoes",
    "eventos_risco",
    "estornos",
]

for fonte in fontes:
    caminho_fonte = f"{CAMINHO_VOLUME}/{fonte}"

    dbutils.fs.mkdirs(caminho_fonte)

    print(f"Diretório validado: {caminho_fonte}")

# COMMAND ----------

# Lista o conteúdo do volume para validar a estrutura criada.

arquivos_volume = dbutils.fs.ls(CAMINHO_VOLUME)

display(arquivos_volume)

# COMMAND ----------

# Configuração central das fontes.
#
# O campo formato poderá ser alterado para json caso a massa CDC seja
# gerada nesse formato.

CONFIGURACAO_FONTES = {
    "clientes": {
        "formato": "csv",
        "caminho": f"{CAMINHO_VOLUME}/clientes",
        "tabela": f"{CATALOGO}.{SCHEMA_BRONZE}.clientes",
    },
    "contas": {
        "formato": "csv",
        "caminho": f"{CAMINHO_VOLUME}/contas",
        "tabela": f"{CATALOGO}.{SCHEMA_BRONZE}.contas",
    },
    "cartoes": {
        "formato": "csv",
        "caminho": f"{CAMINHO_VOLUME}/cartoes",
        "tabela": f"{CATALOGO}.{SCHEMA_BRONZE}.cartoes",
    },
    "transacoes": {
        "formato": "csv",
        "caminho": f"{CAMINHO_VOLUME}/transacoes",
        "tabela": f"{CATALOGO}.{SCHEMA_BRONZE}.transacoes",
    },
    "eventos_risco": {
        "formato": "csv",
        "caminho": f"{CAMINHO_VOLUME}/eventos_risco",
        "tabela": f"{CATALOGO}.{SCHEMA_BRONZE}.eventos_risco",
    },
    "estornos": {
        "formato": "csv",
        "caminho": f"{CAMINHO_VOLUME}/estornos",
        "tabela": f"{CATALOGO}.{SCHEMA_BRONZE}.estornos",
    },
}

for nome_fonte, configuracao in CONFIGURACAO_FONTES.items():
    print(
        nome_fonte,
        configuracao["caminho"],
        configuracao["tabela"],
    )

# COMMAND ----------

def existem_arquivos(caminho: str) -> bool:
    """
    Verifica se existem arquivos disponíveis no caminho informado.

    Pastas vazias retornam False.
    Arquivos e subdiretórios válidos retornam True.
    """

    try:
        itens = dbutils.fs.ls(caminho)

        arquivos_validos = [
            item
            for item in itens
            if not item.name.startswith("_")
            and not item.name.startswith(".")
        ]

        return len(arquivos_validos) > 0

    except Exception as erro:
        print(
            f"Não foi possível listar o caminho {caminho}: {erro}"
        )

        return False

# COMMAND ----------

def ler_arquivos_bronze(
    caminho: str,
    formato: str,
) -> DataFrame:
    """
    Lê os arquivos brutos da fonte e captura os metadados do arquivo.

    A coluna _metadata é disponibilizada pelo Databricks para fontes
    baseadas em arquivos. Ela precisa ser selecionada durante a leitura.
    """

    formato_normalizado = formato.lower().strip()

    if formato_normalizado == "csv":
        dataframe = (
            spark.read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "false")
            .option("mode", "PERMISSIVE")
            .option(
                "columnNameOfCorruptRecord",
                "_registro_corrompido",
            )
            .load(caminho)
        )

    elif formato_normalizado == "json":
        dataframe = (
            spark.read
            .format("json")
            .option("multiLine", "false")
            .option("mode", "PERMISSIVE")
            .option(
                "columnNameOfCorruptRecord",
                "_registro_corrompido",
            )
            .load(caminho)
        )

    else:
        raise ValueError(
            f"Formato não suportado: {formato}. "
            "Utilize csv ou json."
        )

    # A coluna _metadata é oculta e precisa ser selecionada
    # antes das demais transformações.
    return dataframe.select(
        "*",
        F.col("_metadata.file_path").alias(
            "_arquivo_origem_metadata"
        ),
        F.col("_metadata.file_name").alias(
            "_nome_arquivo_metadata"
        ),
        F.col("_metadata.file_size").alias(
            "_tamanho_arquivo_metadata"
        ),
        F.col("_metadata.file_modification_time").alias(
            "_data_modificacao_arquivo_metadata"
        ),
    )

# COMMAND ----------

def adicionar_metadados_bronze(
    dataframe: DataFrame,
    batch_id: str,
    schema_version: str,
) -> DataFrame:
    """
    Adiciona os metadados técnicos da camada Bronze.

    Metadados obrigatórios:
    - arquivo_origem
    - data_ingestao
    - timestamp_ingestao
    - batch_id
    - hash_linha
    - schema_version

    Metadados adicionais:
    - nome_arquivo_origem
    - tamanho_arquivo_origem
    - data_modificacao_arquivo
    """

    # Não incluímos as colunas técnicas do Databricks no hash.
    colunas_tecnicas_metadata = {
        "_arquivo_origem_metadata",
        "_nome_arquivo_metadata",
        "_tamanho_arquivo_metadata",
        "_data_modificacao_arquivo_metadata",
    }

    colunas_origem = sorted(
        [
            nome_coluna
            for nome_coluna in dataframe.columns
            if nome_coluna not in colunas_tecnicas_metadata
        ]
    )

    conteudo_hash = F.concat_ws(
        "||",
        *[
            F.coalesce(
                F.col(nome_coluna).cast("string"),
                F.lit("∅"),
            )
            for nome_coluna in colunas_origem
        ],
    )

    return (
        dataframe
        .withColumn(
            "arquivo_origem",
            F.col("_arquivo_origem_metadata"),
        )
        .withColumn(
            "nome_arquivo_origem",
            F.col("_nome_arquivo_metadata"),
        )
        .withColumn(
            "tamanho_arquivo_origem",
            F.col("_tamanho_arquivo_metadata"),
        )
        .withColumn(
            "data_modificacao_arquivo",
            F.col(
                "_data_modificacao_arquivo_metadata"
            ),
        )
        .withColumn(
            "data_ingestao",
            F.current_date(),
        )
        .withColumn(
            "timestamp_ingestao",
            F.current_timestamp(),
        )
        .withColumn(
            "batch_id",
            F.lit(batch_id),
        )
        .withColumn(
            "hash_linha",
            F.sha2(conteudo_hash, 256),
        )
        .withColumn(
            "schema_version",
            F.lit(schema_version),
        )
        .drop(
            "_arquivo_origem_metadata",
            "_nome_arquivo_metadata",
            "_tamanho_arquivo_metadata",
            "_data_modificacao_arquivo_metadata",
        )
    )

# COMMAND ----------

def ingerir_fonte_bronze(
    nome_fonte: str,
    batch_id: Optional[str] = None,
) -> dict:
    """
    Executa a ingestão de uma fonte para uma tabela Delta Bronze.

    O método utiliza append porque a camada Bronze preserva o histórico
    bruto das cargas. A deduplicação e o MERGE serão realizados na Prata.
    """

    if nome_fonte not in CONFIGURACAO_FONTES:
        raise ValueError(
            f"Fonte não configurada: {nome_fonte}"
        )

    configuracao = CONFIGURACAO_FONTES[nome_fonte]

    caminho = configuracao["caminho"]
    formato = configuracao["formato"]
    tabela_destino = configuracao["tabela"]

    batch_id_execucao = batch_id or str(uuid.uuid4())

    print("=" * 80)
    print(f"Iniciando ingestão da fonte: {nome_fonte}")
    print(f"Caminho: {caminho}")
    print(f"Tabela de destino: {tabela_destino}")
    print(f"Batch ID: {batch_id_execucao}")

    if not existem_arquivos(caminho):
        print(
            f"Nenhum arquivo encontrado para a fonte {nome_fonte}."
        )

        return {
            "fonte": nome_fonte,
            "status": "SEM_ARQUIVOS",
            "batch_id": batch_id_execucao,
            "quantidade_registros": 0,
            "tabela": tabela_destino,
        }

    try:
        dataframe_bruto = ler_arquivos_bronze(
            caminho=caminho,
            formato=formato,
        )

        dataframe_bronze = adicionar_metadados_bronze(
            dataframe=dataframe_bruto,
            batch_id=batch_id_execucao,
            schema_version=SCHEMA_VERSION,
        )

        quantidade_registros = dataframe_bronze.count()

        (
            dataframe_bronze.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(tabela_destino)
        )

        print(
            f"Ingestão concluída: {quantidade_registros} registros."
        )

        return {
            "fonte": nome_fonte,
            "status": "SUCESSO",
            "batch_id": batch_id_execucao,
            "quantidade_registros": quantidade_registros,
            "tabela": tabela_destino,
        }

    except Exception as erro:
        print(
            f"Falha crítica na ingestão da fonte {nome_fonte}: {erro}"
        )

        raise

# COMMAND ----------

# Executa a ingestão para todas as fontes configuradas.

batch_id_pipeline = str(uuid.uuid4())

resultados_ingestao = []

for nome_fonte in CONFIGURACAO_FONTES:
    resultado = ingerir_fonte_bronze(
        nome_fonte=nome_fonte,
        batch_id=batch_id_pipeline,
    )

    resultados_ingestao.append(resultado)

print(f"Execução finalizada. Batch: {batch_id_pipeline}")

# COMMAND ----------

# Converte os resultados da execução em DataFrame para facilitar
# visualização e geração de evidências.

dataframe_resultados = spark.createDataFrame(
    resultados_ingestao
)

display(dataframe_resultados)

# COMMAND ----------

# Lista as tabelas Delta existentes na camada Bronze.

spark.sql(
    f"SHOW TABLES IN {CATALOGO}.{SCHEMA_BRONZE}"
).show(truncate=False)

# COMMAND ----------

# Lista as tabelas Delta existentes na camada Bronze.

spark.sql(
    f"SHOW TABLES IN {CATALOGO}.{SCHEMA_BRONZE}"
).show(truncate=False)

# COMMAND ----------

# Confirma que as tabelas criadas utilizam o formato Delta.

for nome_fonte, configuracao in CONFIGURACAO_FONTES.items():
    tabela = configuracao["tabela"]

    if spark.catalog.tableExists(tabela):
        print(f"Detalhes da tabela: {tabela}")

        spark.sql(
            f"DESCRIBE DETAIL {tabela}"
        ).select(
            "format",
            "location",
            "numFiles",
            "sizeInBytes",
        ).show(truncate=False)