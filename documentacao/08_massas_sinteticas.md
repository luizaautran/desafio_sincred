# 08 — Massa Sintética

## Objetivo

Testar desempenho, CDC, duplicidade e integridade.

## Volumes de referência

```python
QT_CLIENTES = 50_000
QT_CONTAS = 80_000
QT_CARTOES = 120_000
QT_TRANSACOES = 3_000_000
QT_EVENTOS_RISCO = 100_000
QT_ESTORNOS = 50_000
```

## Anomalias intencionais

- Clientes duplicados.
- Contas duplicadas.
- Atualizações CDC.
- CPF inválido.
- Datas inválidas.
- Conta sem cliente.
- Cartão sem conta.
- Transação órfã.
- Valor negativo.
- Estorno sem transação.
- Evolução de schema.

## Interpretação

As anomalias não são defeitos do gerador. Elas existem para testar se cada camada cumpre sua responsabilidade.

## Boas práticas

- Gerar em lotes.
- Evitar `collect`.
- Utilizar seed.
- Não usar dados pessoais reais.
