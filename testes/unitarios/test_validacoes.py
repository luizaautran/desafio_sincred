from pyspark.sql import Row

from produto_transacional.qualidade.validacoes import (
    separar_validos_invalidos,
)


def test_separar_validos_invalidos(spark):
    dados = [
        Row(
            id_cliente=1,
            nome="João",
            motivo_quarentena=None,
        ),
        Row(
            id_cliente=2,
            nome="Maria",
            motivo_quarentena="CPF inválido",
        ),
    ]

    df = spark.createDataFrame(dados)

    df_validos, df_invalidos = separar_validos_invalidos(df)

    assert df_validos.count() == 1
    assert df_invalidos.count() == 1

    assert "motivo_quarentena" not in df_validos.columns
    assert "motivo_quarentena" in df_invalidos.columns

    assert df_validos.first().id_cliente == 1
    assert df_invalidos.first().id_cliente == 2