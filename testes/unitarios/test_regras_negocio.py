from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from produto_transacional.qualidade.validacoes import (
    adicionar_motivo_quarentena,
    separar_validos_invalidos,
)


SCHEMA_CLIENTE = StructType(
    [
        StructField("id_cliente", LongType(), False),
        StructField("cpf", StringType(), True),
        StructField("renda", DoubleType(), True),
        StructField("estado", StringType(), True),
    ]
)


def test_registro_com_multiplas_violacoes_acumula_motivos(spark):
    dataframe = spark.createDataFrame(
        [
            Row(
                id_cliente=1,
                cpf=None,
                renda=-100.0,
                estado=None,
            )
        ],
        schema=SCHEMA_CLIENTE,
    )

    regras = [
        (F.col("cpf").isNull(), "CPF obrigatório"),
        (F.col("renda") < 0, "Renda negativa"),
        (F.col("estado").isNull(), "Estado obrigatório"),
    ]

    resultado = adicionar_motivo_quarentena(
        dataframe,
        regras,
    ).first()

    assert resultado.motivo_quarentena == (
        "CPF obrigatório; Renda negativa; Estado obrigatório"
    )


def test_registro_sem_violacao_permanece_valido(spark):
    dataframe = spark.createDataFrame(
        [
            Row(
                id_cliente=1,
                cpf="12345678901",
                renda=3000.0,
                estado="PE",
            )
        ],
        schema=SCHEMA_CLIENTE,
    )

    regras = [
        (F.col("cpf").isNull(), "CPF obrigatório"),
        (F.col("renda") < 0, "Renda negativa"),
        (F.col("estado").isNull(), "Estado obrigatório"),
    ]

    dataframe_validado = adicionar_motivo_quarentena(
        dataframe,
        regras,
    )

    validos, invalidos = separar_validos_invalidos(
        dataframe_validado
    )

    assert validos.count() == 1
    assert invalidos.count() == 0
    assert validos.first().id_cliente == 1