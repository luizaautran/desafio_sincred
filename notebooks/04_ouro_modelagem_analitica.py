# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Modelagem Analítica da Camada Ouro
# MAGIC
# MAGIC ## Objetivo
# MAGIC
# MAGIC Este notebook é responsável pela construção da **Camada Ouro** da arquitetura Medallion.
# MAGIC
# MAGIC A Camada Ouro organiza os dados tratados da Camada Prata em estruturas analíticas voltadas ao consumo por ferramentas de Business Intelligence, relatórios, indicadores e análises de negócio.
# MAGIC
# MAGIC O processamento contempla:
# MAGIC
# MAGIC - construção de dimensões;
# MAGIC - construção de tabelas fato;
# MAGIC - geração de dimensão calendário;
# MAGIC - consolidação de indicadores por cliente;
# MAGIC - análise de padrões de consumo;
# MAGIC - análise de risco;
# MAGIC - análise de estornos;
# MAGIC - análise de comportamento mensal.
# MAGIC
# MAGIC ## Entradas
# MAGIC
# MAGIC - `workspace.prata.clientes`
# MAGIC - `workspace.prata.contas`
# MAGIC - `workspace.prata.cartoes`
# MAGIC - `workspace.prata.transacoes`
# MAGIC - `workspace.prata.eventos_risco`
# MAGIC - `workspace.prata.estornos`
# MAGIC
# MAGIC ## Saídas
# MAGIC
# MAGIC ### Dimensões
# MAGIC
# MAGIC - `workspace.ouro.dim_clientes`
# MAGIC - `workspace.ouro.dim_contas`
# MAGIC - `workspace.ouro.dim_cartoes`
# MAGIC - `workspace.ouro.dim_data`
# MAGIC
# MAGIC ### Fatos
# MAGIC
# MAGIC - `workspace.ouro.fato_transacoes`
# MAGIC - `workspace.ouro.fato_estornos`
# MAGIC - `workspace.ouro.fato_eventos_risco`
# MAGIC
# MAGIC ### Tabelas analíticas
# MAGIC
# MAGIC - `workspace.ouro.analise_consumo_clientes`
# MAGIC - `workspace.ouro.analise_padrao_consumo`
# MAGIC - `workspace.ouro.analise_risco_clientes`
# MAGIC - `workspace.ouro.analise_estornos_clientes`
# MAGIC - `workspace.ouro.analise_consumo_mensal`
# MAGIC
# MAGIC ## Resultado esperado
# MAGIC
# MAGIC Ao final da execução, a Camada Ouro estará preparada para consultas analíticas, criação de dashboards e geração de indicadores sobre movimentação financeira, comportamento de consumo, risco e estornos.
# MAGIC

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# COMMAND ----------

dbutils.widgets.text("catalogo", "workspace")
dbutils.widgets.text("schema_prata", "prata")
dbutils.widgets.text("schema_ouro", "ouro")

CATALOGO = dbutils.widgets.get("catalogo")
SCHEMA_PRATA = dbutils.widgets.get("schema_prata")
SCHEMA_OURO = dbutils.widgets.get("schema_ouro")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Funções auxiliares

# COMMAND ----------

def tabela_existe(nome_tabela: str) -> bool:
    return spark.catalog.tableExists(nome_tabela)


def gravar_tabela_ouro(
    dataframe: DataFrame,
    nome_tabela: str
) -> None:
    tabela_destino = (
        f"{CATALOGO}.{SCHEMA_OURO}.{nome_tabela}"
    )

    (
        dataframe.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(tabela_destino)
    )

    print(f"Tabela criada: {tabela_destino}")


def ler_registros_atuais(nome_tabela: str) -> DataFrame:
    tabela = spark.table(
        f"{CATALOGO}.{SCHEMA_PRATA}.{nome_tabela}"
    )

    if "registro_atual" in tabela.columns:
        tabela = tabela.filter(
            F.col("registro_atual") == True
        )

    return tabela


# COMMAND ----------

# MAGIC %md
# MAGIC # Dimensões
# MAGIC
# MAGIC As dimensões representam as principais entidades de negócio utilizadas nas análises.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensão Clientes

# COMMAND ----------

clientes = ler_registros_atuais("clientes")

dim_clientes = (
    clientes
    .select(
        "id_cliente",
        "cpf",
        "nome",
        "cidade",
        "estado",
        "renda"
    )
    .dropDuplicates(["id_cliente"])
)

gravar_tabela_ouro(
    dim_clientes,
    "dim_clientes"
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensão Contas

# COMMAND ----------

contas = ler_registros_atuais("contas")

dim_contas = (
    contas
    .select(
        "id_conta",
        "id_cliente",
        "tipo_conta",
        "status_conta"
    )
    .dropDuplicates(["id_conta"])
)

gravar_tabela_ouro(
    dim_contas,
    "dim_contas"
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensão Cartões

# COMMAND ----------

cartoes = ler_registros_atuais("cartoes")

dim_cartoes = (
    cartoes
    .select(
        "id_cartao",
        "id_conta",
        "tipo_cartao",
        "status_cartao",
        "limite"
    )
    .dropDuplicates(["id_cartao"])
)

gravar_tabela_ouro(
    dim_cartoes,
    "dim_cartoes"
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensão Data
# MAGIC
# MAGIC A dimensão calendário é gerada a partir das datas existentes nas transações, estornos e eventos de risco.
# MAGIC

# COMMAND ----------

transacoes_prata = spark.table(
    f"{CATALOGO}.{SCHEMA_PRATA}.transacoes"
)

estornos_prata = spark.table(
    f"{CATALOGO}.{SCHEMA_PRATA}.estornos"
)

eventos_risco_prata = spark.table(
    f"{CATALOGO}.{SCHEMA_PRATA}.eventos_risco"
)

datas_disponiveis = (
    transacoes_prata
    .select(
        F.to_date("data_transacao").alias("data")
    )
    .unionByName(
        estornos_prata.select(
            F.to_date("data_estorno").alias("data")
        )
    )
    .unionByName(
        eventos_risco_prata.select(
            F.to_date("data_evento").alias("data")
        )
    )
    .filter(F.col("data").isNotNull())
)

intervalo_datas = datas_disponiveis.agg(
    F.min("data").alias("data_inicial"),
    F.max("data").alias("data_final")
).first()

if (
    intervalo_datas["data_inicial"] is None
    or intervalo_datas["data_final"] is None
):
    data_inicial = "2025-01-01"
    data_final = "2026-12-31"
else:
    data_inicial = str(intervalo_datas["data_inicial"])
    data_final = str(intervalo_datas["data_final"])

dim_data = (
    spark.sql(f"""
        SELECT explode(
            sequence(
                to_date('{data_inicial}'),
                to_date('{data_final}'),
                interval 1 day
            )
        ) AS data
    """)
    .withColumn(
        "id_data",
        F.date_format("data", "yyyyMMdd").cast("int")
    )
    .withColumn("ano", F.year("data"))
    .withColumn("mes", F.month("data"))
    .withColumn(
        "nome_mes",
        F.date_format("data", "MMMM")
    )
    .withColumn(
        "trimestre",
        F.quarter("data")
    )
    .withColumn(
        "dia",
        F.dayofmonth("data")
    )
    .withColumn(
        "dia_semana",
        F.date_format("data", "EEEE")
    )
    .withColumn(
        "numero_dia_semana",
        F.dayofweek("data")
    )
    .withColumn(
        "fim_de_semana",
        F.when(
            F.dayofweek("data").isin(1, 7),
            F.lit(True)
        ).otherwise(F.lit(False))
    )
    .select(
        "id_data",
        "data",
        "ano",
        "mes",
        "nome_mes",
        "trimestre",
        "dia",
        "dia_semana",
        "numero_dia_semana",
        "fim_de_semana"
    )
)

gravar_tabela_ouro(
    dim_data,
    "dim_data"
)


# COMMAND ----------

# MAGIC %md
# MAGIC # Tabelas Fato
# MAGIC
# MAGIC As tabelas fato armazenam os eventos transacionais e suas relações com as dimensões.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fato Transações

# COMMAND ----------

colunas_transacoes = set(transacoes_prata.columns)

coluna_status = (
    F.col("transacao.status").cast("string")
    if "status" in colunas_transacoes
    else F.lit(None).cast("string")
)

coluna_tipo_transacao = (
    F.col("transacao.tipo_transacao").cast("string")
    if "tipo_transacao" in colunas_transacoes
    else F.lit(None).cast("string")
)

coluna_canal = (
    F.col("transacao.canal").cast("string")
    if "canal" in colunas_transacoes
    else F.lit(None).cast("string")
)

fato_transacoes = (
    transacoes_prata.alias("transacao")
    .join(
        dim_contas.alias("conta"),
        F.col("transacao.id_conta")
        == F.col("conta.id_conta"),
        "inner"
    )
    .select(
        F.col("transacao.id_transacao")
        .cast("string")
        .alias("id_transacao"),

        F.col("conta.id_cliente")
        .cast("string")
        .alias("id_cliente"),

        F.col("transacao.id_conta")
        .cast("string")
        .alias("id_conta"),

        F.col("transacao.id_cartao")
        .cast("string")
        .alias("id_cartao"),

        F.date_format(
            F.to_date(
                F.col("transacao.data_transacao")
            ),
            "yyyyMMdd"
        )
        .cast("int")
        .alias("id_data"),

        F.col("transacao.data_transacao")
        .cast("timestamp")
        .alias("data_transacao"),

        coluna_tipo_transacao.alias("tipo_transacao"),

        coluna_canal.alias("canal"),

        F.col("transacao.valor")
        .cast("decimal(18,2)")
        .alias("valor"),

        coluna_status.alias("status")
    )
    .dropDuplicates(["id_transacao"])
)

gravar_tabela_ouro(
    fato_transacoes,
    "fato_transacoes"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fato Estornos

# COMMAND ----------

fato_estornos = (
    estornos_prata.alias("estorno")
    .join(
        fato_transacoes.alias("transacao"),
        F.col("estorno.id_transacao")
        == F.col("transacao.id_transacao"),
        "inner"
    )
    .select(
        F.col("estorno.id_estorno"),
        F.col("estorno.id_transacao"),
        F.col("transacao.id_cliente"),
        F.date_format(
            F.to_date("estorno.data_estorno"),
            "yyyyMMdd"
        ).cast("int").alias("id_data"),
        F.col("estorno.data_estorno"),
        F.col("estorno.valor_estorno"),
        F.col("estorno.motivo")
        if "motivo" in estornos_prata.columns
        else F.lit(None).cast("string").alias("motivo")
    )
    .dropDuplicates(["id_estorno"])
)

gravar_tabela_ouro(
    fato_estornos,
    "fato_estornos"
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Fato Eventos de Risco

# COMMAND ----------

colunas_eventos = set(eventos_risco_prata.columns)

coluna_severidade = (
    F.col("evento.severidade").cast("string")
    if "severidade" in colunas_eventos
    else F.lit(None).cast("string")
)

coluna_tipo_evento = (
    F.col("evento.tipo_evento").cast("string")
    if "tipo_evento" in colunas_eventos
    else F.lit(None).cast("string")
)

fato_eventos_risco = (
    eventos_risco_prata.alias("evento")
    .join(
        fato_transacoes.alias("transacao"),
        F.col("evento.id_transacao")
        == F.col("transacao.id_transacao"),
        "left"
    )
    .select(
        F.col("evento.id_evento")
        .cast("string")
        .alias("id_evento"),

        F.col("evento.id_transacao")
        .cast("string")
        .alias("id_transacao"),

        F.col("transacao.id_cliente")
        .cast("string")
        .alias("id_cliente"),

        F.col("evento.id_conta")
        .cast("string")
        .alias("id_conta"),

        F.col("evento.id_cartao")
        .cast("string")
        .alias("id_cartao"),

        F.date_format(
            F.to_date(
                F.col("evento.data_evento")
            ),
            "yyyyMMdd"
        )
        .cast("int")
        .alias("id_data"),

        F.col("evento.data_evento")
        .cast("timestamp")
        .alias("data_evento"),

        coluna_severidade.alias("severidade"),

        coluna_tipo_evento.alias("tipo_evento")
    )
    .dropDuplicates(["id_evento"])
)

gravar_tabela_ouro(
    fato_eventos_risco,
    "fato_eventos_risco"
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Tabelas Analíticas
# MAGIC
# MAGIC As tabelas analíticas apresentam indicadores consolidados e prontos para uso por relatórios e dashboards.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Análise de Consumo por Cliente
# MAGIC
# MAGIC Consolida volume financeiro, frequência de movimentação e relacionamento do cliente com contas e cartões.
# MAGIC

# COMMAND ----------

metricas_transacoes_cliente = (
    fato_transacoes
    .groupBy("id_cliente")
    .agg(
        F.countDistinct("id_transacao").alias(
            "quantidade_transacoes"
        ),
        F.sum("valor").alias(
            "valor_total_movimentado"
        ),
        F.avg("valor").alias(
            "valor_medio_transacao"
        ),
        F.max("valor").alias(
            "maior_transacao"
        ),
        F.min("valor").alias(
            "menor_transacao"
        ),
        F.min("data_transacao").alias(
            "data_primeira_transacao"
        ),
        F.max("data_transacao").alias(
            "data_ultima_transacao"
        )
    )
)

metricas_contas_cliente = (
    dim_contas
    .groupBy("id_cliente")
    .agg(
        F.countDistinct("id_conta").alias(
            "quantidade_contas"
        )
    )
)

metricas_cartoes_cliente = (
    dim_cartoes.alias("cartao")
    .join(
        dim_contas.alias("conta"),
        F.col("cartao.id_conta")
        == F.col("conta.id_conta"),
        "left"
    )
    .groupBy(
        F.col("conta.id_cliente").alias("id_cliente")
    )
    .agg(
        F.countDistinct("cartao.id_cartao").alias(
            "quantidade_cartoes"
        )
    )
)

analise_consumo_clientes = (
    dim_clientes.alias("cliente")
    .join(
        metricas_transacoes_cliente.alias("transacao"),
        "id_cliente",
        "left"
    )
    .join(
        metricas_contas_cliente.alias("conta"),
        "id_cliente",
        "left"
    )
    .join(
        metricas_cartoes_cliente.alias("cartao"),
        "id_cliente",
        "left"
    )
    .select(
        "id_cliente",
        "nome",
        "cidade",
        "estado",
        "renda",
        F.coalesce(
            F.col("quantidade_transacoes"),
            F.lit(0)
        ).alias("quantidade_transacoes"),
        F.coalesce(
            F.col("valor_total_movimentado"),
            F.lit(0)
        ).alias("valor_total_movimentado"),
        F.coalesce(
            F.col("valor_medio_transacao"),
            F.lit(0)
        ).alias("valor_medio_transacao"),
        F.col("maior_transacao"),
        F.col("menor_transacao"),
        F.coalesce(
            F.col("quantidade_contas"),
            F.lit(0)
        ).alias("quantidade_contas"),
        F.coalesce(
            F.col("quantidade_cartoes"),
            F.lit(0)
        ).alias("quantidade_cartoes"),
        F.col("data_primeira_transacao"),
        F.col("data_ultima_transacao")
    )
)

gravar_tabela_ouro(
    analise_consumo_clientes,
    "analise_consumo_clientes"
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Análise de Padrão de Consumo
# MAGIC
# MAGIC Apresenta a participação de cada tipo de transação no consumo total do cliente.
# MAGIC

# COMMAND ----------

consumo_por_tipo = (
    fato_transacoes
    .groupBy(
        "id_cliente",
        "tipo_transacao"
    )
    .agg(
        F.countDistinct("id_transacao").alias(
            "quantidade_transacoes"
        ),
        F.sum("valor").alias("valor_total"),
        F.avg("valor").alias("valor_medio")
    )
)

total_cliente = (
    fato_transacoes
    .groupBy("id_cliente")
    .agg(
        F.sum("valor").alias(
            "valor_total_cliente"
        )
    )
)

analise_padrao_consumo = (
    consumo_por_tipo.alias("tipo")
    .join(
        total_cliente.alias("cliente"),
        "id_cliente",
        "left"
    )
    .withColumn(
        "percentual_consumo_cliente",
        F.when(
            F.col("valor_total_cliente") > 0,
            F.round(
                (
                    F.col("valor_total")
                    / F.col("valor_total_cliente")
                ) * 100,
                2
            )
        ).otherwise(F.lit(0))
    )
    .select(
        "id_cliente",
        "tipo_transacao",
        "quantidade_transacoes",
        "valor_total",
        "valor_medio",
        "percentual_consumo_cliente"
    )
)

gravar_tabela_ouro(
    analise_padrao_consumo,
    "analise_padrao_consumo"
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Análise de Risco por Cliente
# MAGIC
# MAGIC Consolida eventos de risco e classifica o cliente conforme a severidade registrada.
# MAGIC

# COMMAND ----------

analise_risco_clientes = (
    dim_clientes.alias("cliente")
    .join(
        fato_eventos_risco.alias("risco"),
        F.col("cliente.id_cliente")
        == F.col("risco.id_cliente"),
        "left"
    )
    .groupBy(
        F.col("cliente.id_cliente"),
        F.col("cliente.nome")
    )
    .agg(
        F.countDistinct(
            F.col("risco.id_evento")
        ).alias(
            "quantidade_eventos_risco"
        ),

        F.sum(
            F.when(
                F.col("risco.severidade") == "CRITICA",
                1
            ).otherwise(0)
        ).alias(
            "quantidade_risco_critico"
        ),

        F.sum(
            F.when(
                F.col("risco.severidade") == "ALTA",
                1
            ).otherwise(0)
        ).alias(
            "quantidade_risco_alto"
        ),

        F.sum(
            F.when(
                F.col("risco.severidade") == "MEDIA",
                1
            ).otherwise(0)
        ).alias(
            "quantidade_risco_medio"
        ),

        F.sum(
            F.when(
                F.col("risco.severidade") == "BAIXA",
                1
            ).otherwise(0)
        ).alias(
            "quantidade_risco_baixo"
        ),

        F.max(
            F.col("risco.data_evento")
        ).alias(
            "data_ultimo_evento_risco"
        )
    )
    .withColumn(
        "classificacao_risco_cliente",
        F.when(
            F.col("quantidade_risco_critico") > 0,
            F.lit("CRITICO")
        )
        .when(
            F.col("quantidade_risco_alto") > 0,
            F.lit("ALTO")
        )
        .when(
            F.col("quantidade_risco_medio") > 0,
            F.lit("MEDIO")
        )
        .when(
            F.col("quantidade_risco_baixo") > 0,
            F.lit("BAIXO")
        )
        .otherwise(
            F.lit("SEM_EVENTOS")
        )
    )
)

gravar_tabela_ouro(
    analise_risco_clientes,
    "analise_risco_clientes"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Análise de Estornos por Cliente
# MAGIC
# MAGIC Consolida quantidade, valor e proporção de transações estornadas.
# MAGIC

# COMMAND ----------

transacoes_por_cliente = (
    fato_transacoes
    .groupBy("id_cliente")
    .agg(
        F.countDistinct("id_transacao").alias(
            "quantidade_total_transacoes"
        )
    )
)

estornos_por_cliente = (
    fato_estornos
    .groupBy("id_cliente")
    .agg(
        F.countDistinct("id_estorno").alias(
            "quantidade_estornos"
        ),
        F.sum("valor_estorno").alias(
            "valor_total_estornado"
        ),
        F.avg("valor_estorno").alias(
            "valor_medio_estorno"
        ),
        F.max("data_estorno").alias(
            "data_ultimo_estorno"
        )
    )
)

analise_estornos_clientes = (
    dim_clientes.alias("cliente")
    .join(
        transacoes_por_cliente.alias("transacao"),
        "id_cliente",
        "left"
    )
    .join(
        estornos_por_cliente.alias("estorno"),
        "id_cliente",
        "left"
    )
    .withColumn(
        "quantidade_total_transacoes",
        F.coalesce(
            F.col("quantidade_total_transacoes"),
            F.lit(0)
        )
    )
    .withColumn(
        "quantidade_estornos",
        F.coalesce(
            F.col("quantidade_estornos"),
            F.lit(0)
        )
    )
    .withColumn(
        "percentual_transacoes_estornadas",
        F.when(
            F.col("quantidade_total_transacoes") > 0,
            F.round(
                (
                    F.col("quantidade_estornos")
                    / F.col("quantidade_total_transacoes")
                ) * 100,
                2
            )
        ).otherwise(F.lit(0))
    )
    .select(
        "id_cliente",
        "nome",
        "quantidade_total_transacoes",
        "quantidade_estornos",
        F.coalesce(
            F.col("valor_total_estornado"),
            F.lit(0)
        ).alias("valor_total_estornado"),
        F.coalesce(
            F.col("valor_medio_estorno"),
            F.lit(0)
        ).alias("valor_medio_estorno"),
        "percentual_transacoes_estornadas",
        "data_ultimo_estorno"
    )
)

gravar_tabela_ouro(
    analise_estornos_clientes,
    "analise_estornos_clientes"
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Análise de Consumo Mensal
# MAGIC
# MAGIC Apresenta o comportamento mensal de consumo e estornos por cliente.
# MAGIC

# COMMAND ----------

transacoes_mensais = (
    fato_transacoes
    .withColumn(
        "ano",
        F.year("data_transacao")
    )
    .withColumn(
        "mes",
        F.month("data_transacao")
    )
    .groupBy(
        "id_cliente",
        "ano",
        "mes"
    )
    .agg(
        F.countDistinct("id_transacao").alias(
            "quantidade_transacoes"
        ),
        F.sum("valor").alias("valor_total"),
        F.avg("valor").alias("valor_medio")
    )
)

estornos_mensais = (
    fato_estornos
    .withColumn(
        "ano",
        F.year("data_estorno")
    )
    .withColumn(
        "mes",
        F.month("data_estorno")
    )
    .groupBy(
        "id_cliente",
        "ano",
        "mes"
    )
    .agg(
        F.countDistinct("id_estorno").alias(
            "quantidade_estornos"
        ),
        F.sum("valor_estorno").alias(
            "valor_estornado"
        )
    )
)

analise_consumo_mensal = (
    transacoes_mensais.alias("transacao")
    .join(
        estornos_mensais.alias("estorno"),
        ["id_cliente", "ano", "mes"],
        "left"
    )
    .select(
        "id_cliente",
        "ano",
        "mes",
        "quantidade_transacoes",
        "valor_total",
        "valor_medio",
        F.coalesce(
            F.col("quantidade_estornos"),
            F.lit(0)
        ).alias("quantidade_estornos"),
        F.coalesce(
            F.col("valor_estornado"),
            F.lit(0)
        ).alias("valor_estornado")
    )
)

gravar_tabela_ouro(
    analise_consumo_mensal,
    "analise_consumo_mensal"
)


# COMMAND ----------

# MAGIC %md
# MAGIC # Validação Final
# MAGIC
# MAGIC Esta seção apresenta a quantidade de registros de cada tabela criada na Camada Ouro.
# MAGIC

# COMMAND ----------

tabelas_ouro = [
    "dim_clientes",
    "dim_contas",
    "dim_cartoes",
    "dim_data",
    "fato_transacoes",
    "fato_estornos",
    "fato_eventos_risco",
    "analise_consumo_clientes",
    "analise_padrao_consumo",
    "analise_risco_clientes",
    "analise_estornos_clientes",
    "analise_consumo_mensal"
]

print("=" * 80)
print("RESUMO DA CAMADA OURO")
print("=" * 80)

for tabela in tabelas_ouro:
    nome_completo = (
        f"{CATALOGO}.{SCHEMA_OURO}.{tabela}"
    )

    if tabela_existe(nome_completo):
        quantidade = spark.table(
            nome_completo
        ).count()

        print(
            f"{tabela:<40} "
            f"{quantidade:>12} registros"
        )
    else:
        print(
            f"{tabela:<40} "
            "NÃO CRIADA"
        )