# Desafio Sincred — Data Product Transacional

Solução desenvolvida para o desafio técnico de Engenharia de Dados Sênior.

O projeto implementa um Data Product transacional utilizando Databricks,
Delta Lake e arquitetura Medallion, com camadas Bronze, Prata e Ouro.

## Objetivos

- Implementar ingestão incremental de arquivos;
- Utilizar Delta Lake como formato persistente;
- Construir camadas Bronze, Prata e Ouro;
- Aplicar regras de qualidade de dados;
- Segregar registros inválidos em quarentena;
- Implementar histórico SCD Tipo 2;
- Garantir idempotência com MERGE;
- Criar produtos de dados para análises e Data Science;
- Demonstrar boas práticas de engenharia de software e governança.

## Arquitetura

```text
Arquivos de origem
        |
        v
Zona de entrada
        |
        v
Camada Bronze
        |
        v
Camada Prata
        |
        v
Camada Ouro
        |
        +--> Análises
        +--> Prevenção de perdas
        +--> Data Science
