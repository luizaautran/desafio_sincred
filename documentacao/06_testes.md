# 06 — Testes

## Categorias

- Existência de tabelas.
- Volume.
- Nulidade.
- Unicidade.
- Integridade referencial.
- Domínio.
- Consistência temporal.
- Consistência financeira.

## Duplicidade de clientes

```python
duplicados = (
    spark.table("workspace.prata.clientes")
    .groupBy("id_cliente")
    .count()
    .filter("count > 1")
    .count()
)
assert duplicados == 0
```

## Integridade de estornos

```python
invalidos = (
    spark.table("workspace.ouro.fato_estornos").alias("e")
    .join(
        spark.table("workspace.ouro.fato_transacoes")
        .select("id_transacao").alias("t"),
        "id_transacao",
        "left_anti"
    )
    .count()
)
assert invalidos == 0
```

## Resultado esperado

Cada teste deve registrar:

- teste;
- tabela;
- status;
- quantidade inválida;
- mensagem;
- data de execução.

Os testes devem detectar falhas, nunca corrigir os dados.
