from uuid import uuid4

from pyspark.sql import Row

from produto_transacional.prata.scd import executar_scd_tipo_2


def _nome_tabela_temporaria(spark):
    catalogo = spark.sql(
        "SELECT current_catalog() AS catalogo"
    ).first().catalogo

    schema = spark.sql(
        "SELECT current_schema() AS schema"
    ).first().schema

    sufixo = uuid4().hex

    return f"`{catalogo}`.`{schema}`.`teste_scd_{sufixo}`"


def test_scd_cria_primeira_versao_ativa(spark):
    tabela = _nome_tabela_temporaria(spark)

    dataframe = spark.createDataFrame(
        [
            Row(
                id_cliente=1,
                nome="Maria",
                renda=2000.0,
            )
        ]
    )

    try:
        executar_scd_tipo_2(
            spark=spark,
            dataframe=dataframe,
            tabela_destino=tabela,
            chaves=["id_cliente"],
            colunas_comparacao=["nome", "renda"],
        )

        registros = spark.table(tabela).collect()

        assert len(registros) == 1
        assert registros[0].id_cliente == 1
        assert registros[0].registro_ativo is True
        assert registros[0].data_inicio_vigencia is not None
        assert registros[0].data_fim_vigencia is None
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {tabela}")


def test_scd_mantem_historico_quando_registro_muda(spark):
    tabela = _nome_tabela_temporaria(spark)

    primeira_versao = spark.createDataFrame(
        [
            Row(
                id_cliente=1,
                nome="Maria",
                renda=2000.0,
            )
        ]
    )

    segunda_versao = spark.createDataFrame(
        [
            Row(
                id_cliente=1,
                nome="Maria",
                renda=2500.0,
            )
        ]
    )

    try:
        executar_scd_tipo_2(
            spark=spark,
            dataframe=primeira_versao,
            tabela_destino=tabela,
            chaves=["id_cliente"],
            colunas_comparacao=["nome", "renda"],
        )

        executar_scd_tipo_2(
            spark=spark,
            dataframe=segunda_versao,
            tabela_destino=tabela,
            chaves=["id_cliente"],
            colunas_comparacao=["nome", "renda"],
        )

        registros = (
            spark.table(tabela)
            .orderBy("data_inicio_vigencia")
            .collect()
        )

        ativos = [
            registro
            for registro in registros
            if registro.registro_ativo
        ]

        inativos = [
            registro
            for registro in registros
            if not registro.registro_ativo
        ]

        assert len(registros) == 2
        assert len(ativos) == 1
        assert len(inativos) == 1

        assert ativos[0].renda == 2500.0
        assert ativos[0].data_fim_vigencia is None

        assert inativos[0].renda == 2000.0
        assert inativos[0].data_fim_vigencia is not None
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {tabela}")


def test_scd_nao_duplica_registro_sem_alteracao(spark):
    tabela = _nome_tabela_temporaria(spark)

    dataframe = spark.createDataFrame(
        [
            Row(
                id_cliente=1,
                nome="Maria",
                renda=2000.0,
            )
        ]
    )

    try:
        for _ in range(2):
            executar_scd_tipo_2(
                spark=spark,
                dataframe=dataframe,
                tabela_destino=tabela,
                chaves=["id_cliente"],
                colunas_comparacao=["nome", "renda"],
            )

        registros = spark.table(tabela).collect()

        assert len(registros) == 1
        assert registros[0].registro_ativo is True
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {tabela}")