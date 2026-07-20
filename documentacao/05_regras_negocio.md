# 05 — Regras de Negócio

## Clientes

- `id_cliente` obrigatório e único na Prata.
- Nome não vazio.
- CPF padronizado.
- Estado com dois caracteres.
- Data de nascimento não futura.
- Manter versão mais recente.

## Contas

- `id_conta` obrigatório e único.
- Conta deve possuir cliente válido.
- Saldo e limite numéricos.
- Status dentro do domínio.

## Cartões

- Cartão deve possuir conta válida.
- Validade posterior à emissão.
- Status dentro do domínio.

## Transações

- `id_transacao` obrigatório.
- Valor maior que zero para consumo normal.
- Cartão e conta válidos.
- Transações órfãs vão para rejeitados.

## Eventos de risco

Níveis aceitos:

- BAIXO
- MEDIO
- ALTO

## Estornos

- Devem referenciar transação existente.
- Valor maior que zero.
- Data não anterior à transação.

## Duplicidade

A Bronze pode conter duplicidade. A Prata deve remover duplicidade exata e manter uma única versão por chave, usando critério determinístico.

## Rejeitados

Campos sugeridos:

- motivo_rejeicao
- regra_violada
- data_rejeicao
- arquivo_origem
- payload_original
