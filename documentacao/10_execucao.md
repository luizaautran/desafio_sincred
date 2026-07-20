# 10 — Guia de Execução

## Pré-requisitos

- Databricks.
- Compute disponível.
- Unity Catalog.
- Permissões de criação e escrita.
- Notebooks importados.

## Criar schemas

```sql
CREATE SCHEMA IF NOT EXISTS workspace.bronze;
CREATE SCHEMA IF NOT EXISTS workspace.prata;
CREATE SCHEMA IF NOT EXISTS workspace.ouro;
CREATE SCHEMA IF NOT EXISTS workspace.monitoramento;
```

## Ordem

1. Executar `00_gerar_massa_sintetica`.
2. Executar `01_bronze_ingestao`.
3. Executar `02_prata_transformacao`.
4. Executar `03_ouro_modelagem`.
5. Executar `04_testes`.
7. Executar `06_orquestrador`.


## Checklist

- [ ] Caminhos ajustados
- [ ] Permissões validadas
- [ ] Bronze carregada
- [ ] Prata sem duplicidade
- [ ] Ouro com integridade
- [ ] Testes aprovados
- [ ] Observabilidade gravada
- [ ] Documentação enviada ao Git
