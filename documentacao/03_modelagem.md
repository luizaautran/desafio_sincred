# 03 — Modelagem de Dados

## Entidades

### Cliente

Chave: `id_cliente`

Relacionamento: um cliente possui várias contas.

### Conta

Chave: `id_conta`  
Chave estrangeira: `id_cliente`

### Cartão

Chave: `id_cartao`  
Chave estrangeira: `id_conta`

### Transação

Chave: `id_transacao`  
Chave estrangeira: `id_cartao`

### Evento de risco

Chave: `id_evento`

Pode estar relacionado a cliente, conta, cartão ou transação.

### Estorno

Chave: `id_estorno`  
Chave estrangeira: `id_transacao`

## Modelo dimensional

Dimensões sugeridas:

- `dim_clientes`
- `dim_contas`
- `dim_cartoes`
- `dim_tempo`

Fatos sugeridos:

- `fato_transacoes`
- `fato_estornos`
- `fato_eventos_risco`

Agregações:

- `analise_risco_clientes`
- `resumo_transacoes_diarias`
- `resumo_movimentacao_contas`

## Integridade

- `fato_transacoes.id_conta` deve existir em `dim_contas`.
- `fato_transacoes.id_cliente` deve existir em `dim_clientes`.
- `fato_estornos.id_transacao` deve existir em `fato_transacoes`.

Registros órfãos devem ir para quarentena.
