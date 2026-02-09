# 🏦 SafeBank AI Agent

> Enterprise-Grade Natural Language to Secure SQL AI Agent for Banking

![Python](https://img.shields.io/badge/Python-3.10+-blue)
![Flask](https://img.shields.io/badge/Flask-Backend-black)
![MySQL](https://img.shields.io/badge/MySQL-8.0-orange)
![Docker](https://img.shields.io/badge/Docker-Containerized-blue)
![License](https://img.shields.io/badge/License-MIT-green)

SafeBank AI Agent is an enterprise-grade AI system designed for banking and financial institutions.  
It securely converts natural language questions into controlled, validated, and compliant SQL queries.

The core objective is to enable non-technical business units and executives to access banking data in a secure, role-based, KVKK/GDPR-compliant, and efficient manner — without requiring SQL knowledge.

---

## 🚨 Problem Statement

In traditional banking environments:

- Business teams do not know SQL and depend on data teams  
- Even simple reports may take hours or days  
- Unauthorized data exposure creates KVKK/GDPR risks  
- Manual query validation increases operational overhead  

This creates a reporting bottleneck that slows decision-making.

---

## 💡 Solution

SafeBank AI Agent introduces a Natural Language → Secure SQL architecture.

Users ask their questions in natural language (Turkish or English).  
The system processes them through a controlled multi-step pipeline:

1. Intent Analysis (metric, time, segmentation, filters)
2. Data Dictionary Validation (table & column verification)
3. Security & Compliance Checks
4. MySQL-Compatible SQL Generation
5. Execution & Natural Language Explanation

Only predefined and authorized fields from the Data Dictionary can be used.

---

## 🔄 System Workflow

- Natural language input (TR / EN)
- Intent & metric extraction
- Data Dictionary validation
- Secure SQL generation
- KVKK & security guard layer
- Query execution (MySQL)
- Result explanation (human-readable)

---

## 🏗 Technical Architecture

### Backend
- Python
- Flask
- Ollama (Local LLM – on-premise compatible)
- Pandas

### Database
- MySQL 8.0

### Infrastructure
- Docker
- Docker Compose

### Data Governance
- CSV-based Data Dictionary
- Column-level validation & restriction
- PII tagging mechanism

---

## 📁 Project Structure

backend/
│
├─ agent/
│   ├─ planner.py
│   ├─ sql_writer.py
│   ├─ guard.py
│   ├─ explainer.py
│   ├─ plan_validator.py
│
├─ catalog/
│   ├─ loader.py
│   ├─ retriever.py
│
├─ db/
│   └─ mysql.py
│
├─ app.py
├─ requirements.txt
├─ docker-compose.yml
├─ seed.sql
├─ data_dictionary.csv

---

## ⚙ Installation

### Requirements
- Docker
- Docker Compose
- Python 3.10+
- Ollama (local environment)

### Start MySQL Service

docker-compose up -d

### Install Python Dependencies

pip install -r requirements.txt

### Run Flask Application

python app.py

---

## 🧪 Example Use Case

Question:

How many private banking customers were there per branch as of 31.12.2025?

System Behavior:

- Validates relevant tables & columns  
- Applies private banking filter  
- Applies snapshot date condition  
- Groups by branch  
- Generates secure SQL  
- Returns table output with explanation  

---

## 🔐 Security & Compliance

- Unauthorized tables and columns are blocked  
- Sensitive fields (PII) are flagged  
- Query limits and filter validation enforced  
- SQL injection protection layer  
- Only Data Dictionary–approved fields are usable  

Designed with KVKK and GDPR compliance principles at its core.

---

## 🎯 Use Cases

- Executive-level reporting  
- Self-service analytics for business teams  
- Internal audit & compliance units  
- Hackathon / PoC demonstrations  
- Enterprise AI Agent integrations  

---

## 🚀 Future Enhancements

- Built-in visualization engine  
- Role-based access control (RBAC)  
- Prompt & query logging  
- PDF / Excel export support  
- Enterprise API gateway integration  

---

## 👨‍💻 Developers

Uğur Emir Azı  
Nisa Ataş  

Computer Engineering  
AI • FinTech • NLP • Data Systems
