# 09 — Decisões Técnicas

## Medallion

Separa ingestão, tratamento e consumo analítico.

## Delta Lake

Escolhido por ACID, evolução de schema, versionamento e MERGE.

## Deduplicação na Prata

A Bronze preserva o dado recebido; a Prata define o registro vigente.

## Integridade na Ouro

Fatos só devem conter relacionamentos válidos.

## Testes automatizados

Falhas críticas interrompem o pipeline.

## Parâmetros

Caminhos, volumes e ambientes devem ser parametrizados.

## Escrita

- `overwrite` para carga completa.
- `MERGE` para CDC.
- Cuidado com `append` em reprocessamento.

## Quarentena

Registros inválidos devem permanecer rastreáveis.
