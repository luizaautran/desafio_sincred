# 07 — Monitoramento e Observabilidade

## Objetivo

Registrar notebook, início, fim, duração, status, volumes e erro.

## Tabela sugerida

`workspace.monitoramento.execucao_notebooks`

## Campos

- execucao_id
- pipeline
- nome_notebook
- caminho_notebook
- status_execucao
- data_inicio
- data_fim
- duracao_segundos
- quantidade_lida
- quantidade_gravada
- resultado
- mensagem_erro

## Schema explícito

Utilizar `StructType` para evitar erro de inferência quando existirem campos `None`.

## Métricas

### Bronze

- Arquivos processados.
- Registros ingeridos.

### Prata

- Registros válidos.
- Duplicidades removidas.
- Rejeitados.

### Ouro

- Fatos carregados.
- Órfãos descartados.

### Testes

- Total, aprovados e reprovados.

## Alertas

- Falha de notebook.
- Volume anormal.
- Taxa de rejeição elevada.
- SLA excedido.
- Integridade quebrada.
