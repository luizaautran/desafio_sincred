from unittest.mock import MagicMock

from produto_transacional.prata import merge as modulo_merge


def test_executar_merge_configura_operacao_delta(monkeypatch):
    dataframe = MagicMock()
    dataframe.sparkSession = MagicMock()
    dataframe.alias.return_value = "dataframe_origem"

    delta_table = MagicMock()
    tabela_alias = MagicMock()
    merge_builder = MagicMock()
    update_builder = MagicMock()
    insert_builder = MagicMock()

    delta_table.alias.return_value = tabela_alias
    tabela_alias.merge.return_value = merge_builder
    merge_builder.whenMatchedUpdate.return_value = update_builder
    update_builder.whenNotMatchedInsertAll.return_value = insert_builder

    for_name = MagicMock(return_value=delta_table)

    monkeypatch.setattr(
        modulo_merge.DeltaTable,
        "forName",
        for_name,
    )

    colunas_atualizacao = {
        "nome": "origem.nome",
        "renda": "origem.renda",
    }

    modulo_merge.executar_merge(
        dataframe=dataframe,
        tabela_destino="workspace.prata.clientes",
        condicao_merge="destino.id_cliente = origem.id_cliente",
        colunas_atualizacao=colunas_atualizacao,
    )

    for_name.assert_called_once_with(
        dataframe.sparkSession,
        "workspace.prata.clientes",
    )
    delta_table.alias.assert_called_once_with("destino")
    dataframe.alias.assert_called_once_with("origem")
    tabela_alias.merge.assert_called_once_with(
        "dataframe_origem",
        "destino.id_cliente = origem.id_cliente",
    )
    merge_builder.whenMatchedUpdate.assert_called_once_with(
        set=colunas_atualizacao
    )
    update_builder.whenNotMatchedInsertAll.assert_called_once_with()
    insert_builder.execute.assert_called_once_with()