# 02 — Arquitetura

## Visão geral

```text
CSV → Bronze → Prata → Ouro → Testes/Monitoramento
```

## Bronze

Responsável por armazenar o dado com o mínimo de transformação.

### Deve fazer

- Leitura dos arquivos.
- Inclusão de metadados.
- Preservação do histórico.
- Rastreabilidade do arquivo de origem.

### Não deve fazer

- Deduplicação definitiva.
- Aplicação de regras de negócio.
- Descarte silencioso.

## Prata

Responsável por:

- conversão de tipos;
- padronização;
- validação;
- deduplicação;
- CDC;
- quarentena de registros inválidos.

## Ouro

Responsável por:

- dimensões;
- fatos;
- agregações;
- indicadores;
- integridade analítica.

## Orquestração

Pode ser realizada por `dbutils.notebook.run` ou Databricks Workflows.

## Governança recomendada

- Unity Catalog.
- Permissões mínimas.
- Ambientes separados.
- Service principal em produção.
- Secrets para credenciais.
