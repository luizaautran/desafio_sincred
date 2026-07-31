import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession


RAIZ_PROJETO = Path(__file__).resolve().parents[1]
CAMINHO_CODIGO = RAIZ_PROJETO / "codigo"

sys.path.insert(0, str(CAMINHO_CODIGO))


@pytest.fixture(scope="session")
def spark():
    sessao = SparkSession.getActiveSession()

    if sessao is None:
        sessao = SparkSession.builder.getOrCreate()

    return sessao