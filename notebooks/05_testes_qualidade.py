# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Testes de qualidade
# MAGIC
# MAGIC Valida existência, volume, unicidade, obrigatoriedade, integridade referencial, domínios e valores financeiros nas camadas Prata e Ouro.

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

CATALOGO = "workspace"
SCHEMA_PRATA = "prata"
SCHEMA_OURO = "ouro"
SCHEMA_QUARENTENA = "quarentena"

falhas = []
resultados = []

# COMMAND ----------

# MAGIC %md
# MAGIC ## Funções auxiliares

# COMMAND ----------

def tabela_existe(nome_tabela):
    try:
        spark.table(nome_tabela).limit(1).collect()
        return True
    except Exception:
        return False


def registrar_teste(nome_teste, aprovado, detalhe=""):
    status = "APROVADO" if aprovado else "REPROVADO"
    resultados.append({
        "teste": nome_teste,
        "status": status,
        "detalhe": detalhe,
        "data_execucao": datetime.now()
    })
    print(f"{'✅' if aprovado else '❌'} {nome_teste}: {status}")
    if detalhe:
        print(f"   {detalhe}")
    if not aprovado:
        falhas.append(nome_teste)


def validar_tabela_existe(nome_tabela):
    registrar_teste(f"Tabela existe: {nome_tabela}", tabela_existe(nome_tabela))


def validar_tabela_nao_vazia(nome_tabela):
    if not tabela_existe(nome_tabela):
        registrar_teste(f"Tabela possui registros: {nome_tabela}", False, "A tabela não existe.")
        return
    quantidade = spark.table(nome_tabela).count()
    registrar_teste(f"Tabela possui registros: {nome_tabela}", quantidade > 0, f"Quantidade: {quantidade}")


def validar_sem_duplicidade(nome_tabela, colunas_chave):
    if not tabela_existe(nome_tabela):
        registrar_teste(
            f"Sem duplicidade: {nome_tabela}",
            False,
            "A tabela não existe."
        )
        return

    df = spark.table(nome_tabela)

    if "registro_atual" in df.columns:
        df = df.filter(F.col("registro_atual") == True)

    duplicados = (
        df
        .groupBy(*colunas_chave)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    registrar_teste(
        f"Sem duplicidade: {nome_tabela}",
        duplicados == 0,
        f"Grupos duplicados: {duplicados}"
    )

def validar_sem_nulos(nome_tabela, colunas_obrigatorias):
    if not tabela_existe(nome_tabela):
        registrar_teste(f"Campos obrigatórios: {nome_tabela}", False, "A tabela não existe.")
        return
    df = spark.table(nome_tabela)
    ausentes = [c for c in colunas_obrigatorias if c not in df.columns]
    if ausentes:
        registrar_teste(f"Campos obrigatórios: {nome_tabela}", False, f"Colunas ausentes: {ausentes}")
        return
    condicao = None
    for c in colunas_obrigatorias:
        regra = F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == "")
        condicao = regra if condicao is None else condicao | regra
    invalidos = df.filter(condicao).count()
    registrar_teste(f"Campos obrigatórios preenchidos: {nome_tabela}", invalidos == 0, f"Registros inválidos: {invalidos}")


def validar_integridade_referencial(tabela_fato, coluna_fato, tabela_dimensao, coluna_dimensao):
    if not tabela_existe(tabela_fato) or not tabela_existe(tabela_dimensao):
        registrar_teste(f"Integridade: {tabela_fato}.{coluna_fato}", False, "Tabela fato ou dimensão não existe.")
        return
    fato = spark.table(tabela_fato).select(F.col(coluna_fato).cast("string").alias("chave")).filter(F.col("chave").isNotNull()).dropDuplicates()
    dim = spark.table(tabela_dimensao).select(F.col(coluna_dimensao).cast("string").alias("chave_dim")).filter(F.col("chave_dim").isNotNull()).dropDuplicates()
    orfaos = fato.join(dim, F.col("chave") == F.col("chave_dim"), "left_anti").count()
    registrar_teste(f"Integridade: {tabela_fato}.{coluna_fato}", orfaos == 0, f"Chaves órfãs: {orfaos}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Existência e volume

# COMMAND ----------

tabelas_obrigatorias = [
    f"{CATALOGO}.{SCHEMA_PRATA}.clientes",
    f"{CATALOGO}.{SCHEMA_PRATA}.contas",
    f"{CATALOGO}.{SCHEMA_PRATA}.cartoes",
    f"{CATALOGO}.{SCHEMA_PRATA}.transacoes",
    f"{CATALOGO}.{SCHEMA_PRATA}.eventos_risco",
    f"{CATALOGO}.{SCHEMA_PRATA}.estornos",
    f"{CATALOGO}.{SCHEMA_OURO}.dim_clientes",
    f"{CATALOGO}.{SCHEMA_OURO}.dim_contas",
    f"{CATALOGO}.{SCHEMA_OURO}.fato_transacoes",
    f"{CATALOGO}.{SCHEMA_OURO}.fato_eventos_risco",
    f"{CATALOGO}.{SCHEMA_OURO}.fato_estornos",
    f"{CATALOGO}.{SCHEMA_OURO}.analise_risco_clientes"
]
for tabela in tabelas_obrigatorias:
    validar_tabela_existe(tabela)

for tabela in [
    f"{CATALOGO}.{SCHEMA_PRATA}.clientes",
    f"{CATALOGO}.{SCHEMA_PRATA}.contas",
    f"{CATALOGO}.{SCHEMA_PRATA}.cartoes",
    f"{CATALOGO}.{SCHEMA_PRATA}.transacoes",
    f"{CATALOGO}.{SCHEMA_OURO}.dim_clientes",
    f"{CATALOGO}.{SCHEMA_OURO}.dim_contas",
    f"{CATALOGO}.{SCHEMA_OURO}.fato_transacoes"
]:
    validar_tabela_nao_vazia(tabela)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unicidade, obrigatoriedade e valores

# COMMAND ----------

for tabela, chave in [
    (f"{CATALOGO}.{SCHEMA_PRATA}.clientes", ["id_cliente"]),
    (f"{CATALOGO}.{SCHEMA_PRATA}.contas", ["id_conta"]),
    (f"{CATALOGO}.{SCHEMA_PRATA}.cartoes", ["id_cartao"]),
    (f"{CATALOGO}.{SCHEMA_PRATA}.transacoes", ["id_transacao"]),
    (f"{CATALOGO}.{SCHEMA_PRATA}.eventos_risco", ["id_evento"]),
    (f"{CATALOGO}.{SCHEMA_PRATA}.estornos", ["id_estorno"]),
    (f"{CATALOGO}.{SCHEMA_OURO}.fato_transacoes", ["id_transacao"]),
    (f"{CATALOGO}.{SCHEMA_OURO}.fato_eventos_risco", ["id_evento"]),
    (f"{CATALOGO}.{SCHEMA_OURO}.fato_estornos", ["id_estorno"])
]:
    validar_sem_duplicidade(tabela, chave)

validar_sem_nulos(f"{CATALOGO}.{SCHEMA_PRATA}.transacoes", ["id_transacao", "id_cartao", "id_conta", "data_transacao", "valor"])
validar_sem_nulos(f"{CATALOGO}.{SCHEMA_PRATA}.eventos_risco", ["id_evento", "id_transacao", "tipo_evento", "severidade", "data_evento"])
validar_sem_nulos(f"{CATALOGO}.{SCHEMA_PRATA}.estornos", ["id_estorno", "id_transacao", "valor_estorno", "data_estorno"])

fato_t = spark.table(f"{CATALOGO}.{SCHEMA_OURO}.fato_transacoes")
invalidos = fato_t.filter(F.col("valor").isNull() | (F.col("valor") <= 0)).count()
registrar_teste("Transações possuem valor positivo", invalidos == 0, f"Registros inválidos: {invalidos}")

fato_e = spark.table(f"{CATALOGO}.{SCHEMA_OURO}.fato_estornos")
invalidos = fato_e.filter(F.col("valor_estorno").isNull() | (F.col("valor_estorno") <= 0)).count()
registrar_teste("Estornos possuem valor positivo", invalidos == 0, f"Registros inválidos: {invalidos}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Integridade e domínio de risco

# COMMAND ----------

validar_integridade_referencial(f"{CATALOGO}.{SCHEMA_OURO}.fato_transacoes", "id_cliente", f"{CATALOGO}.{SCHEMA_OURO}.dim_clientes", "id_cliente")
validar_integridade_referencial(f"{CATALOGO}.{SCHEMA_OURO}.fato_transacoes", "id_conta", f"{CATALOGO}.{SCHEMA_OURO}.dim_contas", "id_conta")
validar_integridade_referencial(f"{CATALOGO}.{SCHEMA_OURO}.fato_eventos_risco", "id_transacao", f"{CATALOGO}.{SCHEMA_OURO}.fato_transacoes", "id_transacao")
validar_integridade_referencial(f"{CATALOGO}.{SCHEMA_OURO}.fato_estornos", "id_transacao", f"{CATALOGO}.{SCHEMA_OURO}.fato_transacoes", "id_transacao")

risco = spark.table(f"{CATALOGO}.{SCHEMA_OURO}.fato_eventos_risco")
severidades_validas = ["BAIXA", "MEDIA", "ALTA", "CRITICA"]
invalidas = risco.filter(F.col("severidade").isNull() | ~F.col("severidade").isin(severidades_validas)).count()
registrar_teste("Severidades possuem domínio válido", invalidas == 0, f"Registros inválidos: {invalidas}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resultado final

# COMMAND ----------

resultado_df = spark.createDataFrame(resultados)
display(resultado_df.orderBy("status", "teste"))

print("=" * 70)
print(f"Total de testes: {len(resultados)}")
print(f"Aprovados: {len(resultados) - len(falhas)}")
print(f"Reprovados: {len(falhas)}")

if falhas:
    raise AssertionError("Falhas encontradas: " + ", ".join(falhas))

print("Todos os testes foram aprovados com sucesso.")