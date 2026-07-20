# Databricks notebook source
# MAGIC %md
# MAGIC # 99 — Orquestração do pipeline
# MAGIC
# MAGIC Executa os notebooks em sequência. Ajuste o caminho-base para o seu Workspace.

# COMMAND ----------

from datetime import datetime
import json

CATALOGO = "workspace"
SCHEMA_OBSERVABILIDADE = "observabilidade"

CAMINHO_BASE_NOTEBOOKS = "/Workspace/Users/luizaautran@gmail.com/"
EXECUTAR_GERACAO_MASSA = False
EXECUTAR_CONSULTAS = True
TEMPO_LIMITE_SEGUNDOS = 3600

parametros_comuns = {
    "catalogo": "workspace",
    "schema_bronze": "bronze",
    "schema_prata": "prata",
    "schema_ouro": "ouro",
    "schema_quarentena": "quarentena",
    "schema_observabilidade": "observabilidade"
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Funções auxiliares

# COMMAND ----------

def executar_notebook(nome_notebook, parametros=None, tempo_limite=TEMPO_LIMITE_SEGUNDOS):
    caminho = f"{CAMINHO_BASE_NOTEBOOKS}/{nome_notebook}"
    inicio = datetime.now()
    print("=" * 80)
    print(f"INICIANDO: {nome_notebook}")
    print(f"Caminho: {caminho}")
    try:
        retorno = dbutils.notebook.run(caminho, tempo_limite, parametros or {})
        fim = datetime.now()
        duracao = (fim - inicio).total_seconds()
        print(f"✅ Finalizado em {duracao:.2f} segundos.")
        return {"notebook": nome_notebook, "status": "SUCESSO", "inicio": inicio.isoformat(), "fim": fim.isoformat(), "duracao_segundos": duracao, "retorno": retorno, "erro": None}
    except Exception as erro:
        fim = datetime.now()
        duracao = (fim - inicio).total_seconds()
        print(f"❌ Erro em {nome_notebook}: {erro}")
        raise RuntimeError(f"Falha no notebook {nome_notebook}: {erro}") from erro


def gravar_execucao_pipeline(resultados):
    if not resultados:
        return
    df = spark.createDataFrame(resultados)
    (df.write.format("delta").mode("append").option("mergeSchema", "true")
       .saveAsTable(f"{CATALOGO}.{SCHEMA_OBSERVABILIDADE}.execucoes_pipeline"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ordem de execução

# COMMAND ----------

notebooks_pipeline = []
if EXECUTAR_GERACAO_MASSA:
    notebooks_pipeline.append("00_gerar_massa_sintetica")

notebooks_pipeline.extend([
    "01_bronze_ingestao",
    "02_controle_arquivos_processados",
    "03_prata_transformacao",
    "04_ouro_modelagem_analitica",
    "05_testes_qualidade"
])


# COMMAND ----------

# MAGIC %md
# MAGIC ## Execução

# COMMAND ----------

resultados_execucao = []
inicio_pipeline = datetime.now()
status_pipeline = "SUCESSO"

try:
    for nome_notebook in notebooks_pipeline:
        resultados_execucao.append(executar_notebook(nome_notebook, parametros_comuns))
except Exception as erro_pipeline:
    status_pipeline = "ERRO"
    resultados_execucao.append({
        "notebook": "PIPELINE",
        "status": "ERRO",
        "inicio": inicio_pipeline.isoformat(),
        "fim": datetime.now().isoformat(),
        "duracao_segundos": (datetime.now() - inicio_pipeline).total_seconds(),
        "retorno": None,
        "erro": str(erro_pipeline)
    })
    raise
finally:
    try:
        gravar_execucao_pipeline(resultados_execucao)
    except Exception as erro_observabilidade:
        print(f"Não foi possível gravar a observabilidade: {erro_observabilidade}")

fim_pipeline = datetime.now()
duracao_pipeline = (fim_pipeline - inicio_pipeline).total_seconds()
print("=" * 80)
print(f"Status: {status_pipeline}")
print(f"Duração total: {duracao_pipeline:.2f} segundos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo

# COMMAND ----------

if resultados_execucao:
    display(spark.createDataFrame(resultados_execucao))

dbutils.notebook.exit(json.dumps({
    "status": status_pipeline,
    "inicio": inicio_pipeline.isoformat(),
    "fim": fim_pipeline.isoformat(),
    "duracao_segundos": duracao_pipeline,
    "notebooks_executados": len(resultados_execucao)
}, ensure_ascii=False))