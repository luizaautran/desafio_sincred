# 01 — Visão Geral

## Contexto

O Desafio Sincred simula um cenário financeiro com arquivos de clientes, contas, cartões, transações, eventos de risco e estornos. A massa contém registros válidos e anomalias intencionais para demonstrar qualidade de dados.

## Objetivo geral

Transformar dados brutos em informações confiáveis para consumo analítico.

## Objetivos específicos

- Gerar massa sintética.
- Ingerir arquivos com rastreabilidade.
- Aplicar tipagem e padronização.
- Tratar duplicidades e CDC.
- Validar integridade entre entidades.
- Criar modelo dimensional.
- Automatizar testes.
- Registrar execuções e falhas.

## Escopo

Inclui as camadas Bronze, Prata e Ouro, testes, consultas, orquestração e observabilidade.

## Fora do escopo atual

- Dados bancários reais.
- Streaming em produção.
- Machine Learning produtivo.
- Dashboard publicado.
- CI/CD completo.

## Critérios de sucesso

- Prata sem duplicidade de chave.
- Ouro com integridade referencial.
- Testes aprovados.
- Pipeline executável em sequência.
- Documentação suficiente para reprodução por outro profissional.
