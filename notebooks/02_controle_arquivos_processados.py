# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Controle de Arquivos Processados
# MAGIC
# MAGIC Este notebook controla o ciclo de vida dos arquivos utilizados na ingestão Bronze.
# MAGIC
# MAGIC ## Responsabilidades
# MAGIC
# MAGIC - Identificar os arquivos disponíveis nas pastas de entrada;
# MAGIC - Confirmar se os arquivos foram gravados nas tabelas Bronze;
# MAGIC - Mover arquivos processados para a pasta `processados`;
# MAGIC - Preservar a organização por fonte;
# MAGIC - Registrar o histórico das movimentações;
# MAGIC - Evitar o reprocessamento acidental dos mesmos arquivos.
# MAGIC
# MAGIC ## Regra principal
# MAGIC
# MAGIC Um arquivo somente será movido para `processados` quando existir pelo menos
# MAGIC um registro correspondente na tabela Bronze, identificado pela coluna
# MAGIC `arquivo_origem`.

# COMMAND ----------

from datetime import datetime
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# COMMAND ----------

CATALOGO = "workspace"

SCHEMA_BRONZE = "bronze"
SCHEMA_OBSERVABILIDADE = "observabilidade"

VOLUME_ENTRADA = "arquivos_entrada"

CAMINHO_BASE = (
    f"/Volumes/{CATALOGO}/{SCHEMA_BRONZE}/{VOLUME_ENTRADA}"
)

CAMINHO_PROCESSADOS = f"{CAMINHO_BASE}/processados"
CAMINHO_ERROS = f"{CAMINHO_BASE}/erros"

TABELA_CONTROLE = (
    f"{CATALOGO}.{SCHEMA_OBSERVABILIDADE}."
    "controle_arquivos_processados"
)

print(f"Caminho de entrada: {CAMINHO_BASE}")
print(f"Caminho de processados: {CAMINHO_PROCESSADOS}")
print(f"Tabela de controle: {TABELA_CONTROLE}")

# COMMAND ----------

CONFIGURACAO_FONTES = {
    "clientes": {
        "tabela_bronze": (
            f"{CATALOGO}.{SCHEMA_BRONZE}.clientes"
        ),
    },
    "contas": {
        "tabela_bronze": (
            f"{CATALOGO}.{SCHEMA_BRONZE}.contas"
        ),
    },
    "cartoes": {
        "tabela_bronze": (
            f"{CATALOGO}.{SCHEMA_BRONZE}.cartoes"
        ),
    },
    "transacoes": {
        "tabela_bronze": (
            f"{CATALOGO}.{SCHEMA_BRONZE}.transacoes"
        ),
    },
    "eventos_risco": {
        "tabela_bronze": (
            f"{CATALOGO}.{SCHEMA_BRONZE}.eventos_risco"
        ),
    },
    "estornos": {
        "tabela_bronze": (
            f"{CATALOGO}.{SCHEMA_BRONZE}.estornos"
        ),
    },
}

# COMMAND ----------

for nome_fonte in CONFIGURACAO_FONTES:
    caminho_processados_fonte = (
        f"{CAMINHO_PROCESSADOS}/{nome_fonte}"
    )

    caminho_erros_fonte = (
        f"{CAMINHO_ERROS}/{nome_fonte}"
    )

    dbutils.fs.mkdirs(caminho_processados_fonte)
    dbutils.fs.mkdirs(caminho_erros_fonte)

    print(
        f"Diretórios preparados para a fonte: {nome_fonte}"
    )

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABELA_CONTROLE}
(
    nome_fonte STRING,
    nome_arquivo STRING,
    caminho_origem STRING,
    caminho_destino STRING,
    tabela_bronze STRING,
    status_processamento STRING,
    mensagem STRING,
    data_processamento TIMESTAMP
)
USING DELTA
""")

print(f"Tabela criada ou validada: {TABELA_CONTROLE}")

# COMMAND ----------

def normalizar_caminho(caminho: str) -> str:
    """
    Normaliza caminhos retornados pelo Databricks.

    Exemplo:
    dbfs:/Volumes/workspace/... -> /Volumes/workspace/...
    """

    if caminho is None:
        return ""

    caminho_normalizado = caminho.strip()

    if caminho_normalizado.startswith("dbfs:"):
        caminho_normalizado = caminho_normalizado.replace(
            "dbfs:",
            "",
            1,
        )

    return caminho_normalizado

# COMMAND ----------

def listar_arquivos_entrada(
    nome_fonte: str,
) -> list[dict[str, Any]]:
    """
    Lista somente arquivos presentes na pasta de entrada da fonte.

    Subdiretórios não são considerados arquivos de ingestão.
    """

    caminho_fonte = f"{CAMINHO_BASE}/{nome_fonte}"

    try:
        itens = dbutils.fs.ls(caminho_fonte)

    except Exception as erro:
        print(
            f"Não foi possível acessar a fonte "
            f"{nome_fonte}: {erro}"
        )

        return []

    arquivos = []

    for item in itens:
        # No Databricks, diretórios normalmente terminam com "/".
        if item.name.endswith("/"):
            continue

        arquivos.append(
            {
                "nome_arquivo": item.name,
                "caminho": normalizar_caminho(item.path),
                "tamanho_bytes": item.size,
            }
        )

    return arquivos

# COMMAND ----------

def tabela_existe(nome_tabela: str) -> bool:
    """
    Verifica se uma tabela está registrada no catálogo.
    """

    return spark.catalog.tableExists(nome_tabela)

# COMMAND ----------

def obter_arquivos_registrados_bronze(
    tabela_bronze: str,
) -> set[str]:
    """
    Recupera os caminhos distintos presentes na coluna arquivo_origem
    da tabela Bronze.
    """

    if not tabela_existe(tabela_bronze):
        print(
            f"Tabela Bronze não encontrada: {tabela_bronze}"
        )

        return set()

    dataframe_bronze = spark.table(tabela_bronze)

    if "arquivo_origem" not in dataframe_bronze.columns:
        print(
            f"A tabela {tabela_bronze} não possui "
            "a coluna arquivo_origem."
        )

        return set()

    registros = (
        dataframe_bronze
        .select("arquivo_origem")
        .where(
            F.col("arquivo_origem").isNotNull()
        )
        .distinct()
        .collect()
    )

    return {
        normalizar_caminho(
            registro["arquivo_origem"]
        )
        for registro in registros
    }

# COMMAND ----------

def caminho_existe(caminho: str) -> bool:
    """
    Verifica se um caminho existe no Databricks File System.
    """

    try:
        dbutils.fs.ls(caminho)
        return True

    except Exception:
        return False

# COMMAND ----------

def gerar_caminho_destino(
    nome_fonte: str,
    nome_arquivo: str,
) -> str:
    """
    Gera o caminho de destino do arquivo processado.

    Caso já exista um arquivo com o mesmo nome, adiciona um timestamp
    ao novo arquivo.
    """

    caminho_destino = (
        f"{CAMINHO_PROCESSADOS}/"
        f"{nome_fonte}/"
        f"{nome_arquivo}"
    )

    if not caminho_existe(caminho_destino):
        return caminho_destino

    timestamp = datetime.now().strftime(
        "%Y%m%d_%H%M%S_%f"
    )

    if "." in nome_arquivo:
        nome_base, extensao = nome_arquivo.rsplit(".", 1)

        novo_nome = (
            f"{nome_base}_{timestamp}.{extensao}"
        )

    else:
        novo_nome = (
            f"{nome_arquivo}_{timestamp}"
        )

    return (
        f"{CAMINHO_PROCESSADOS}/"
        f"{nome_fonte}/"
        f"{novo_nome}"
    )

# COMMAND ----------

def registrar_controle(
    nome_fonte: str,
    nome_arquivo: str,
    caminho_origem: str,
    caminho_destino: str,
    tabela_bronze: str,
    status_processamento: str,
    mensagem: str,
) -> None:
    """
    Registra o resultado da movimentação na tabela de observabilidade.
    """

    dados_controle = [
        {
            "nome_fonte": nome_fonte,
            "nome_arquivo": nome_arquivo,
            "caminho_origem": caminho_origem,
            "caminho_destino": caminho_destino,
            "tabela_bronze": tabela_bronze,
            "status_processamento": (
                status_processamento
            ),
            "mensagem": mensagem,
            "data_processamento": datetime.now(),
        }
    ]

    schema_controle = StructType(
        [
            StructField(
                "nome_fonte",
                StringType(),
                False,
            ),
            StructField(
                "nome_arquivo",
                StringType(),
                False,
            ),
            StructField(
                "caminho_origem",
                StringType(),
                False,
            ),
            StructField(
                "caminho_destino",
                StringType(),
                True,
            ),
            StructField(
                "tabela_bronze",
                StringType(),
                False,
            ),
            StructField(
                "status_processamento",
                StringType(),
                False,
            ),
            StructField(
                "mensagem",
                StringType(),
                True,
            ),
            StructField(
                "data_processamento",
                TimestampType(),
                False,
            ),
        ]
    )

    dataframe_controle = spark.createDataFrame(
        dados_controle,
        schema=schema_controle,
    )

    (
        dataframe_controle.write
        .format("delta")
        .mode("append")
        .saveAsTable(TABELA_CONTROLE)
    )

# COMMAND ----------

def processar_arquivos_fonte(
    nome_fonte: str,
) -> list[dict[str, Any]]:
    """
    Verifica quais arquivos da fonte foram gravados na Bronze
    e move somente os arquivos confirmados.
    """

    configuracao = CONFIGURACAO_FONTES[nome_fonte]
    tabela_bronze = configuracao["tabela_bronze"]

    print("=" * 80)
    print(f"Fonte: {nome_fonte}")
    print(f"Tabela Bronze: {tabela_bronze}")

    arquivos_entrada = listar_arquivos_entrada(
        nome_fonte
    )

    if not arquivos_entrada:
        print("Nenhum arquivo encontrado na entrada.")

        return []

    arquivos_bronze = obter_arquivos_registrados_bronze(
        tabela_bronze
    )

    resultados = []

    for arquivo in arquivos_entrada:
        nome_arquivo = arquivo["nome_arquivo"]
        caminho_origem = normalizar_caminho(
            arquivo["caminho"]
        )

        caminho_destino = ""

        try:
            if caminho_origem not in arquivos_bronze:
                status = "NAO_CONFIRMADO"

                mensagem = (
                    "O arquivo não foi encontrado na coluna "
                    "arquivo_origem da tabela Bronze."
                )

                print(
                    f"Não movido: {nome_arquivo} — "
                    f"{mensagem}"
                )

            else:
                caminho_destino = gerar_caminho_destino(
                    nome_fonte=nome_fonte,
                    nome_arquivo=nome_arquivo,
                )

                movimentado = dbutils.fs.mv(
                    caminho_origem,
                    caminho_destino,
                    False,
                )

                if movimentado:
                    status = "PROCESSADO"

                    mensagem = (
                        "Arquivo confirmado na Bronze e "
                        "movido com sucesso."
                    )

                    print(
                        f"Movido: {nome_arquivo}"
                    )

                else:
                    status = "ERRO_MOVIMENTACAO"

                    mensagem = (
                        "O Databricks não confirmou a "
                        "movimentação do arquivo."
                    )

                    print(
                        f"Falha ao mover: {nome_arquivo}"
                    )

        except Exception as erro:
            status = "ERRO"

            mensagem = str(erro)

            print(
                f"Erro ao processar {nome_arquivo}: "
                f"{mensagem}"
            )

        registrar_controle(
            nome_fonte=nome_fonte,
            nome_arquivo=nome_arquivo,
            caminho_origem=caminho_origem,
            caminho_destino=caminho_destino,
            tabela_bronze=tabela_bronze,
            status_processamento=status,
            mensagem=mensagem,
        )

        resultados.append(
            {
                "nome_fonte": nome_fonte,
                "nome_arquivo": nome_arquivo,
                "status": status,
                "caminho_origem": caminho_origem,
                "caminho_destino": caminho_destino,
                "mensagem": mensagem,
            }
        )

    return resultados

# COMMAND ----------

resultados_movimentacao = []

for nome_fonte in CONFIGURACAO_FONTES:
    resultados_fonte = processar_arquivos_fonte(
        nome_fonte
    )

    resultados_movimentacao.extend(
        resultados_fonte
    )

# COMMAND ----------

if resultados_movimentacao:
    dataframe_resultados = spark.createDataFrame(
        resultados_movimentacao
    )

    display(
        dataframe_resultados
        .select(
            "nome_fonte",
            "nome_arquivo",
            "status",
            "caminho_destino",
            "mensagem",
        )
        .orderBy(
            "nome_fonte",
            "nome_arquivo",
        )
    )

else:
    print(
        "Nenhum arquivo foi encontrado para processamento."
    )

# COMMAND ----------

validacao_entrada = []

for nome_fonte in CONFIGURACAO_FONTES:
    arquivos_restantes = listar_arquivos_entrada(
        nome_fonte
    )

    validacao_entrada.append(
        {
            "nome_fonte": nome_fonte,
            "arquivos_restantes": len(
                arquivos_restantes
            ),
        }
    )

display(
    spark.createDataFrame(validacao_entrada)
    .orderBy("nome_fonte")
)

# COMMAND ----------

arquivos_processados = []

for nome_fonte in CONFIGURACAO_FONTES:
    caminho_fonte = (
        f"{CAMINHO_PROCESSADOS}/{nome_fonte}"
    )

    for item in dbutils.fs.ls(caminho_fonte):
        if item.name.endswith("/"):
            continue

        arquivos_processados.append(
            {
                "nome_fonte": nome_fonte,
                "nome_arquivo": item.name,
                "caminho": item.path,
                "tamanho_bytes": item.size,
            }
        )

display(
    spark.createDataFrame(
        arquivos_processados
    ).orderBy(
        "nome_fonte",
        "nome_arquivo",
    )
)

# COMMAND ----------

display(
    spark.table(TABELA_CONTROLE)
    .orderBy(
        F.col("data_processamento").desc()
    )
)

# COMMAND ----------

display(
    spark.table(TABELA_CONTROLE)
    .groupBy(
        "nome_fonte",
        "status_processamento",
    )
    .agg(
        F.count("*").alias(
            "quantidade_arquivos"
        ),
        F.max(
            "data_processamento"
        ).alias(
            "ultima_execucao"
        ),
    )
    .orderBy(
        "nome_fonte",
        "status_processamento",
    )
)