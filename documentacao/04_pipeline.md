# 04 — Pipeline

## Fluxo

```text
00 Massa Sintética
    ↓
01 Bronze
    ↓
02 Prata
    ↓
03 Ouro
    ↓
04 Testes
    ↓
06 Orquestrador
```

## 00 — Massa sintética

Gera clientes, contas, cartões, transações, eventos de risco e estornos, incluindo anomalias controladas.

## 01 — Bronze

Lê arquivos, adiciona metadados e grava tabelas Delta brutas.

## 02 — Prata

Converte tipos, valida domínios, trata duplicidades e CDC e registra rejeitados.

A duplicidade deve ser tratada neste notebook.

## 03 — Ouro

Cria dimensões, fatos e indicadores e mantém integridade referencial.

## 04 — Testes

Valida existência, volume, nulidade, unicidade, integridade, domínio e consistência.


## 06 — Orquestrador

Executa os notebooks em ordem, registra duração, resultado e falhas.

## Idempotência

- `overwrite` para carga completa.
- `MERGE` para CDC.
- Evitar `append` em reprocessamento integral.
