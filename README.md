# Desafio Técnico – Engenharia de Dados

## Objetivo

Este projeto implementa uma pipeline de engenharia de dados para processamento de informações financeiras, contemplando ingestão, tratamento, validação, modelagem analítica e monitoramento, seguindo uma arquitetura em camadas (Medallion).

A solução foi desenvolvida utilizando Databricks, PySpark e Delta Lake, priorizando qualidade dos dados, rastreabilidade, organização do código e facilidade de manutenção.

---

# Arquitetura

A pipeline está organizada nas seguintes camadas:

- **Bronze:** ingestão dos dados brutos preservando a origem.
- **Prata:** limpeza, padronização, validações, deduplicação e aplicação das regras de negócio.
- **Ouro:** disponibilização dos dados prontos para consumo analítico.

Além das camadas principais, a solução possui:

- Camada de Quarentena para registros inválidos;
- Observabilidade das execuções;
- Controle de arquivos processados;
- Testes automatizados.

---

# Tecnologias Utilizadas

- Python
- PySpark
- Delta Lake
- Databricks
- SQL
- Pytest
- Git

---

# Estrutura do Projeto

```
desafio_sincred/
│
├── notebooks/
├── src/
├── testes/
├── documentacao/
├── requirements.txt
├── pyproject.toml
└── README.md
```

---

# Fluxo da Pipeline

```
Arquivos CSV
      │
      ▼
 Bronze
      │
      ▼
 Validações
      │
      ├────────► Quarentena
      │
      ▼
 Prata (MERGE + SCD Tipo 2)
      │
      ▼
 Ouro
      │
      ▼
 Consumo Analítico
```

---

# Principais Melhorias Implementadas

Além dos requisitos propostos no desafio, foram implementadas melhorias visando maior robustez e qualidade da solução:

- Arquitetura Medallion (Bronze, Prata e Ouro);
- Modularização do código em pacotes Python;
- Parametrização dos notebooks;
- Controle de arquivos processados;
- Validação e tratamento de registros inválidos;
- Camada de Quarentena;
- Observabilidade da pipeline;
- Implementação de Slowly Changing Dimension (SCD Tipo 2);
- Deduplicação utilizando Delta Lake MERGE;
- Testes de qualidade dos dados;
- Testes unitários automatizados utilizando Pytest.

---

# Testes

Foi desenvolvida uma suíte de testes unitários para validar os principais componentes da solução.

Cobertura dos testes:

- Regras de negócio;
- Validações;
- Operações de MERGE;
- Implementação do SCD Tipo 2.

**Resultado da execução:**

- 7 testes executados;
- 7 testes aprovados;
- 0 falhas.

---

# Documentação

A documentação complementar encontra-se na pasta **documentacao/**, incluindo:

- Visão geral;
- Arquitetura;
- Modelagem;
- Pipeline;
- Regras de negócio;
- Monitoramento;
- Decisões técnicas;
- Evidências da implementação.

---

# Evidências

As evidências da execução da solução estão disponíveis em:

```
documentacao/evidencias/
```

Incluindo:

- Estrutura do repositório;
- Execução da pipeline;
- Testes automatizados;
- Catálogo Bronze;
- Catálogo Prata;
- Catálogo Ouro;
- Observabilidade;
- Quarentena.

---

# Autor

**Luiza Autran**

Engenheira de Dados