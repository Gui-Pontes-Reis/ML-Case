# ETL Pipeline & Data Modeling - Mercado Libre (Argentina)

Este repositório contém a solução desenvolvida para o desafio técnico de Engenharia de Dados do Mercado Libre. O projeto implementa um pipeline ETL automatizado em Python para extração de dados públicos de anúncios de "Samsung Galaxy S23" na Argentina, aplicando modelagem relacional, limpeza de dados e persistência em banco de dados local.

---

## Arquitetura do Projeto

O fluxo de dados foi estruturado em três camadas (ETL):
1. **Extract (Extração):** Conexão via HTTP com a API pública de busca do Mercado Libre (`/sites/MLA/search`), com paginação em lotes de 50 registros e filtros restritos a produtos novos (`condition=new`). Inclui mecanismo resiliente de fallback para tratamento de restrições de WAF/rede.
2. **Transform (Transformação):** Processamento com Pandas para padronização de colunas, tratamento booleano de garantias, conversão de moedas (ARS para USD) e injeção do campo obrigatório de auditoria `JOB_RUN` (DATETIME).
3. **Load (Carga):** Persistência estruturada em um banco de dados relacional SQLite (`mercado_libre.db`).

---

## Tecnologias Utilizadas
* Python 3.x
* Requests (Requisições HTTP à API)
* Pandas (Manipulação e transformação de dados)
* SQLite (Armazenamento relacional e DDL)

---

## Instruções de Execução

### 1. Clonar o Repositório
```bash
git clone [https://github.com/seu-usuario/ml-etl-case.git](https://github.com/seu-usuario/ml-etl-case.git)
cd ml-etl-case