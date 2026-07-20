# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Geração da Massa Sintética
# MAGIC
# MAGIC Este notebook gera dados fictícios para o Data Product Transacional.
# MAGIC
# MAGIC ## Cenários simulados
# MAGIC
# MAGIC - Atualizações cadastrais em formato CDC;
# MAGIC - Registros duplicados;
# MAGIC - CPF, estado e valores inválidos;
# MAGIC - Clientes com várias contas;
# MAGIC - Contas sem cliente válido;
# MAGIC - Cartões sem conta válida;
# MAGIC - Mudanças de status e limite;
# MAGIC - Cartões cancelados;
# MAGIC - Transações duplicadas em cargas diferentes;
# MAGIC - Arquivos recebidos fora de ordem;
# MAGIC - Transações com valores inconsistentes;
# MAGIC - Evolução de schema;
# MAGIC - Eventos de fraude, suspeita e chargeback;
# MAGIC - Estornos parciais e totais;
# MAGIC - Referências inválidas.
# MAGIC
# MAGIC Todos os dados são sintéticos e não representam pessoas reais.

# COMMAND ----------

import csv
import os
import shutil

from datetime import datetime
from typing import Any

CATALOGO = "workspace"
SCHEMA_BRONZE = "bronze"
NOME_VOLUME = "arquivos_entrada"

CAMINHO_BASE = (
    f"/Volumes/{CATALOGO}/{SCHEMA_BRONZE}/{NOME_VOLUME}"
)

FONTES = [
    "clientes",
    "contas",
    "cartoes",
    "transacoes",
    "eventos_risco",
    "estornos",
]

print(f"Caminho da massa sintética: {CAMINHO_BASE}")

# COMMAND ----------

for fonte in FONTES:
    caminho_fonte = os.path.join(CAMINHO_BASE, fonte)

    if os.path.exists(caminho_fonte):
        shutil.rmtree(caminho_fonte)

    os.makedirs(caminho_fonte, exist_ok=True)

    print(f"Diretório preparado: {caminho_fonte}")

# COMMAND ----------

def gravar_csv(
    caminho: str,
    registros: list[dict[str, Any]],
) -> None:
    """
    Grava uma lista de dicionários em um arquivo CSV.

    A união das chaves permite gerar arquivos com evolução de schema.
    """

    if not registros:
        raise ValueError("A lista de registros não pode estar vazia.")

    colunas = []

    for registro in registros:
        for coluna in registro:
            if coluna not in colunas:
                colunas.append(coluna)

    os.makedirs(
        os.path.dirname(caminho),
        exist_ok=True,
    )

    with open(
        caminho,
        mode="w",
        newline="",
        encoding="utf-8",
    ) as arquivo:
        escritor = csv.DictWriter(
            arquivo,
            fieldnames=colunas,
            extrasaction="ignore",
        )

        escritor.writeheader()
        escritor.writerows(registros)

    print(
        f"Arquivo criado: {caminho} "
        f"({len(registros)} registros)"
    )

# COMMAND ----------

clientes_carga_1 = [
    {
        "id_cliente": "CLI001",
        "cpf": "11111111111",
        "nome": "Ana Silva",
        "cidade": "Recife",
        "estado": "PE",
        "renda": "6500.00",
        "segmento": "ALTA_RENDA",
        "data_atualizacao": "2026-01-05 08:00:00",
        "operacao": "I",
    },
    {
        "id_cliente": "CLI002",
        "cpf": "22222222222",
        "nome": "Bruno Santos",
        "cidade": "Olinda",
        "estado": "PE",
        "renda": "3200.00",
        "segmento": "VAREJO",
        "data_atualizacao": "2026-01-05 08:05:00",
        "operacao": "I",
    },
    {
        "id_cliente": "CLI003",
        "cpf": "33333333333",
        "nome": "Carla Souza",
        "cidade": "Paulista",
        "estado": "PE",
        "renda": "4800.00",
        "segmento": "VAREJO",
        "data_atualizacao": "2026-01-05 08:10:00",
        "operacao": "I",
    },
    {
        "id_cliente": "CLI004",
        "cpf": "44444444444",
        "nome": "Diego Lima",
        "cidade": "Recife",
        "estado": "PE",
        "renda": "12000.00",
        "segmento": "ALTA_RENDA",
        "data_atualizacao": "2026-01-05 08:15:00",
        "operacao": "I",
    },
    {
        "id_cliente": "CLI005",
        "cpf": "CPF_INVALIDO",
        "nome": "",
        "cidade": "Recife",
        "estado": "PERNAMBUCO",
        "renda": "-500.00",
        "segmento": "DESCONHECIDO",
        "data_atualizacao": "2026-01-05 08:20:00",
        "operacao": "I",
    },
]

gravar_csv(
    f"{CAMINHO_BASE}/clientes/clientes_cdc_20260105.csv",
    clientes_carga_1,
)

# COMMAND ----------

clientes_carga_2 = [
    {
        "id_cliente": "CLI001",
        "cpf": "11111111111",
        "nome": "Ana Silva",
        "cidade": "Recife",
        "estado": "PE",
        "renda": "7200.00",
        "segmento": "ALTA_RENDA",
        "data_atualizacao": "2026-02-10 10:00:00",
        "operacao": "U",
    },
    {
        "id_cliente": "CLI002",
        "cpf": "22222222222",
        "nome": "Bruno Santos",
        "cidade": "Jaboatao dos Guararapes",
        "estado": "PE",
        "renda": "3500.00",
        "segmento": "VAREJO",
        "data_atualizacao": "2026-02-10 10:05:00",
        "operacao": "U",
    },
    {
        "id_cliente": "CLI006",
        "cpf": "66666666666",
        "nome": "Fernanda Costa",
        "cidade": "Recife",
        "estado": "PE",
        "renda": "8900.00",
        "segmento": "ALTA_RENDA",
        "data_atualizacao": "2026-02-10 10:10:00",
        "operacao": "I",
    },
    # Duplicidade exata proposital
    {
        "id_cliente": "CLI006",
        "cpf": "66666666666",
        "nome": "Fernanda Costa",
        "cidade": "Recife",
        "estado": "PE",
        "renda": "8900.00",
        "segmento": "ALTA_RENDA",
        "data_atualizacao": "2026-02-10 10:10:00",
        "operacao": "I",
    },
]

gravar_csv(
    f"{CAMINHO_BASE}/clientes/clientes_cdc_20260210.csv",
    clientes_carga_2,
)

# COMMAND ----------

contas_carga_1 = [
    {
        "id_conta": "CON001",
        "id_cliente": "CLI001",
        "tipo_conta": "CORRENTE",
        "status_conta": "ATIVA",
        "data_abertura": "2024-01-10",
        "data_atualizacao": "2026-01-05 09:00:00",
        "operacao": "I",
    },
    {
        "id_conta": "CON002",
        "id_cliente": "CLI001",
        "tipo_conta": "POUPANCA",
        "status_conta": "ATIVA",
        "data_abertura": "2024-03-15",
        "data_atualizacao": "2026-01-05 09:05:00",
        "operacao": "I",
    },
    {
        "id_conta": "CON003",
        "id_cliente": "CLI002",
        "tipo_conta": "CORRENTE",
        "status_conta": "ATIVA",
        "data_abertura": "2025-02-20",
        "data_atualizacao": "2026-01-05 09:10:00",
        "operacao": "I",
    },
    {
        "id_conta": "CON004",
        "id_cliente": "CLI003",
        "tipo_conta": "CORRENTE",
        "status_conta": "ATIVA",
        "data_abertura": "2025-05-01",
        "data_atualizacao": "2026-01-05 09:15:00",
        "operacao": "I",
    },
    {
        "id_conta": "CON005",
        "id_cliente": "CLI999",
        "tipo_conta": "CORRENTE",
        "status_conta": "ATIVA",
        "data_abertura": "2026-01-05",
        "data_atualizacao": "2026-01-05 09:20:00",
        "operacao": "I",
    },
]

contas_carga_2 = [
    {
        "id_conta": "CON003",
        "id_cliente": "CLI002",
        "tipo_conta": "CORRENTE",
        "status_conta": "ENCERRADA",
        "data_abertura": "2025-02-20",
        "data_atualizacao": "2026-03-01 12:00:00",
        "operacao": "U",
    },
    {
        "id_conta": "CON006",
        "id_cliente": "CLI004",
        "tipo_conta": "CORRENTE",
        "status_conta": "ATIVA",
        "data_abertura": "2026-02-01",
        "data_atualizacao": "2026-03-01 12:05:00",
        "operacao": "I",
    },
]

gravar_csv(
    f"{CAMINHO_BASE}/contas/contas_cdc_20260105.csv",
    contas_carga_1,
)

gravar_csv(
    f"{CAMINHO_BASE}/contas/contas_cdc_20260301.csv",
    contas_carga_2,
)

# COMMAND ----------

cartoes_carga_1 = [
    {
        "id_cartao": "CAR001",
        "id_conta": "CON001",
        "tipo_cartao": "CREDITO",
        "limite": "5000.00",
        "status_cartao": "ATIVO",
        "data_atualizacao": "2026-01-05 10:00:00",
        "operacao": "I",
    },
    {
        "id_cartao": "CAR002",
        "id_conta": "CON001",
        "tipo_cartao": "DEBITO",
        "limite": "0.00",
        "status_cartao": "ATIVO",
        "data_atualizacao": "2026-01-05 10:05:00",
        "operacao": "I",
    },
    {
        "id_cartao": "CAR003",
        "id_conta": "CON003",
        "tipo_cartao": "CREDITO",
        "limite": "2500.00",
        "status_cartao": "ATIVO",
        "data_atualizacao": "2026-01-05 10:10:00",
        "operacao": "I",
    },
    {
        "id_cartao": "CAR004",
        "id_conta": "CON004",
        "tipo_cartao": "CREDITO",
        "limite": "4000.00",
        "status_cartao": "ATIVO",
        "data_atualizacao": "2026-01-05 10:15:00",
        "operacao": "I",
    },
    {
        "id_cartao": "CAR999",
        "id_conta": "CON999",
        "tipo_cartao": "CREDITO",
        "limite": "1000.00",
        "status_cartao": "ATIVO",
        "data_atualizacao": "2026-01-05 10:20:00",
        "operacao": "I",
    },
]

cartoes_carga_2 = [
    {
        "id_cartao": "CAR001",
        "id_conta": "CON001",
        "tipo_cartao": "CREDITO",
        "limite": "8000.00",
        "status_cartao": "ATIVO",
        "data_atualizacao": "2026-02-15 11:00:00",
        "operacao": "U",
    },
    {
        "id_cartao": "CAR003",
        "id_conta": "CON003",
        "tipo_cartao": "CREDITO",
        "limite": "2500.00",
        "status_cartao": "CANCELADO",
        "data_atualizacao": "2026-02-20 15:00:00",
        "operacao": "U",
    },
    {
        "id_cartao": "CAR005",
        "id_conta": "CON006",
        "tipo_cartao": "CREDITO",
        "limite": "-100.00",
        "status_cartao": "ATIVO",
        "data_atualizacao": "2026-03-01 12:30:00",
        "operacao": "I",
    },
]

gravar_csv(
    f"{CAMINHO_BASE}/cartoes/cartoes_cdc_20260105.csv",
    cartoes_carga_1,
)

gravar_csv(
    f"{CAMINHO_BASE}/cartoes/cartoes_cdc_20260301.csv",
    cartoes_carga_2,
)

# COMMAND ----------

transacoes_janeiro = [
    {
        "id_transacao": "TRX001",
        "id_cartao": "CAR001",
        "data_transacao": "2026-01-10 09:30:00",
        "valor": "120.50",
        "mcc": "5411",
        "estabelecimento": "Mercado Boa Compra",
        "canal": "POS",
        "pais": "BR",
        "moeda": "BRL",
    },
    {
        "id_transacao": "TRX002",
        "id_cartao": "CAR001",
        "data_transacao": "2026-01-10 09:35:00",
        "valor": "130.00",
        "mcc": "5411",
        "estabelecimento": "Mercado Boa Compra",
        "canal": "POS",
        "pais": "BR",
        "moeda": "BRL",
    },
    {
        "id_transacao": "TRX003",
        "id_cartao": "CAR003",
        "data_transacao": "2026-01-15 14:00:00",
        "valor": "850.00",
        "mcc": "5732",
        "estabelecimento": "Eletronicos Center",
        "canal": "ECOMMERCE",
        "pais": "BR",
        "moeda": "BRL",
    },
    {
        "id_transacao": "TRX004",
        "id_cartao": "CAR004",
        "data_transacao": "2026-01-20 20:10:00",
        "valor": "75.90",
        "mcc": "5812",
        "estabelecimento": "Restaurante Central",
        "canal": "POS",
        "pais": "BR",
        "moeda": "BRL",
    },
    {
        "id_transacao": "TRX005",
        "id_cartao": "CAR999",
        "data_transacao": "2026-01-25 08:00:00",
        "valor": "200.00",
        "mcc": "5411",
        "estabelecimento": "Comercio Invalido",
        "canal": "POS",
        "pais": "BR",
        "moeda": "BRL",
    },
    {
        "id_transacao": "TRX006",
        "id_cartao": "CAR002",
        "data_transacao": "2026-01-26 08:00:00",
        "valor": "-30.00",
        "mcc": "5411",
        "estabelecimento": "Mercado Boa Compra",
        "canal": "POS",
        "pais": "BR",
        "moeda": "BRL",
    },
]

gravar_csv(
    f"{CAMINHO_BASE}/transacoes/transacoes_2026_01.csv",
    transacoes_janeiro,
)

# COMMAND ----------

transacoes_fevereiro = [
    {
        "id_transacao": "TRX007",
        "id_cartao": "CAR001",
        "data_transacao": "2026-02-05 10:00:00",
        "valor": "95.00",
        "mcc": "5411",
        "estabelecimento": "Mercado Boa Compra",
        "canal": "POS",
        "pais": "BR",
        "moeda": "BRL",
    },
    {
        "id_transacao": "TRX008",
        "id_cartao": "CAR001",
        "data_transacao": "2026-02-10 02:15:00",
        "valor": "7800.00",
        "mcc": "6011",
        "estabelecimento": "Saque Internacional",
        "canal": "ATM",
        "pais": "US",
        "moeda": "USD",
    },
    {
        "id_transacao": "TRX009",
        "id_cartao": "CAR003",
        "data_transacao": "2026-02-18 16:00:00",
        "valor": "500.00",
        "mcc": "5732",
        "estabelecimento": "Eletronicos Center",
        "canal": "ECOMMERCE",
        "pais": "BR",
        "moeda": "BRL",
    },
    {
        "id_transacao": "TRX010",
        "id_cartao": "CAR003",
        "data_transacao": "2026-02-25 16:00:00",
        "valor": "300.00",
        "mcc": "5732",
        "estabelecimento": "Loja Apos Cancelamento",
        "canal": "ECOMMERCE",
        "pais": "BR",
        "moeda": "BRL",
    },
    # Repetição do TRX003 em outra carga
    {
        "id_transacao": "TRX003",
        "id_cartao": "CAR003",
        "data_transacao": "2026-01-15 14:00:00",
        "valor": "850.00",
        "mcc": "5732",
        "estabelecimento": "Eletronicos Center",
        "canal": "ECOMMERCE",
        "pais": "BR",
        "moeda": "BRL",
    },
]

gravar_csv(
    f"{CAMINHO_BASE}/transacoes/transacoes_2026_02.csv",
    transacoes_fevereiro,
)

# COMMAND ----------

transacoes_atrasadas = [
    {
        "id_transacao": "TRX011",
        "id_cartao": "CAR004",
        "data_transacao": "2026-01-12 11:00:00",
        "valor": "45.00",
        "mcc": "5814",
        "estabelecimento": "Lanchonete Recife",
        "canal": "APP",
        "pais": "BR",
        "moeda": "BRL",
        "dispositivo": "ANDROID",
    },
    {
        "id_transacao": "TRX012",
        "id_cartao": "CAR001",
        "data_transacao": "2026-03-02 03:00:00",
        "valor": "6200.00",
        "mcc": "6011",
        "estabelecimento": "Saque Madrugada",
        "canal": "ATM",
        "pais": "BR",
        "moeda": "BRL",
        "dispositivo": "ATM-REC-001",
    },
]

gravar_csv(
    f"{CAMINHO_BASE}/transacoes/transacoes_atrasadas_recebidas_20260305.csv",
    transacoes_atrasadas,
)

# COMMAND ----------

eventos_risco = [
    {
        "id_evento": "EVT001",
        "id_transacao": "TRX008",
        "tipo_evento": "SUSPEITA_FRAUDE",
        "severidade": "ALTA",
        "data_evento": "2026-02-10 02:16:00",
    },
    {
        "id_evento": "EVT002",
        "id_transacao": "TRX008",
        "tipo_evento": "TRANSACAO_INTERNACIONAL",
        "severidade": "MEDIA",
        "data_evento": "2026-02-10 02:16:30",
    },
    {
        "id_evento": "EVT003",
        "id_transacao": "TRX003",
        "tipo_evento": "CHARGEBACK",
        "severidade": "ALTA",
        "data_evento": "2026-02-20 12:00:00",
    },
    {
        "id_evento": "EVT004",
        "id_transacao": "TRX012",
        "tipo_evento": "VALOR_ATIPICO",
        "severidade": "ALTA",
        "data_evento": "2026-03-02 03:01:00",
    },
    {
        "id_evento": "EVT005",
        "id_transacao": "TRX999",
        "tipo_evento": "SUSPEITA_FRAUDE",
        "severidade": "CRITICA",
        "data_evento": "2026-03-02 04:00:00",
    },
    {
        "id_evento": "EVT006",
        "id_transacao": "TRX007",
        "tipo_evento": "ANALISE_MANUAL",
        "severidade": "INVALIDA",
        "data_evento": "DATA_INVALIDA",
    },
]

gravar_csv(
    f"{CAMINHO_BASE}/eventos_risco/eventos_risco.csv",
    eventos_risco,
)

# COMMAND ----------

estornos = [
    {
        "id_estorno": "EST001",
        "id_transacao": "TRX002",
        "data_estorno": "2026-01-12 10:00:00",
        "motivo": "SOLICITACAO_CLIENTE",
        "valor_estorno": "30.00",
    },
    {
        "id_estorno": "EST002",
        "id_transacao": "TRX003",
        "data_estorno": "2026-02-20 12:10:00",
        "motivo": "CHARGEBACK",
        "valor_estorno": "850.00",
    },
    {
        "id_estorno": "EST003",
        "id_transacao": "TRX008",
        "data_estorno": "2026-02-11 09:00:00",
        "motivo": "FRAUDE_CONFIRMADA",
        "valor_estorno": "7800.00",
    },
    {
        "id_estorno": "EST004",
        "id_transacao": "TRX999",
        "data_estorno": "2026-03-01 10:00:00",
        "motivo": "REFERENCIA_INVALIDA",
        "valor_estorno": "100.00",
    },
    {
        "id_estorno": "EST005",
        "id_transacao": "TRX007",
        "data_estorno": "DATA_INVALIDA",
        "motivo": "",
        "valor_estorno": "-20.00",
    },
]

gravar_csv(
    f"{CAMINHO_BASE}/estornos/estornos.csv",
    estornos,
)

# COMMAND ----------

for fonte in FONTES:
    caminho = f"{CAMINHO_BASE}/{fonte}"

    print("=" * 80)
    print(f"Fonte: {fonte}")

    for item in dbutils.fs.ls(caminho):
        print(
            f"Arquivo: {item.name} | "
            f"Tamanho: {item.size} bytes"
        )

# COMMAND ----------

resumo_arquivos = []

for fonte in FONTES:
    caminho = f"{CAMINHO_BASE}/{fonte}"

    arquivos = [
        item
        for item in dbutils.fs.ls(caminho)
        if item.name.endswith(".csv")
    ]

    resumo_arquivos.append(
        {
            "fonte": fonte,
            "quantidade_arquivos": len(arquivos),
            "caminho": caminho,
        }
    )

display(
    spark.createDataFrame(resumo_arquivos)
)