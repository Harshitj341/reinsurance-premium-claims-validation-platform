# Reinsurance Premium & Claims Validation Platform

![Architecture](docs/architecture.png)

## Overview

The Reinsurance Premium & Claims Validation Platform is an end-to-end data engineering solution designed to automate the ingestion, governance, validation, reconciliation, and promotion of Life & Health reinsurance data.

The platform addresses common operational challenges faced by reinsurance organizations, including:

- Schema drift across reporting periods
- Historical premium and claims adjustments
- Inconsistent client reporting standards
- Treaty compliance validation
- Data quality failures
- Duplicate submissions
- Personally Identifiable Information (PII) protection
- Statement of Account (SOA) reconciliation

Built using Apache Spark, Apache Airflow, Delta Lake, PostgreSQL, MinIO, Flask, and Docker, the platform combines modern data engineering patterns with operational governance workflows to create a scalable, auditable, and metadata-driven processing framework.

The solution follows a Medallion Architecture (Bronze → Silver → Gold) and incorporates Human-in-the-Loop schema governance, data quality controls, operational workflows, and financial reconciliation processes.

---

## Business Problem

Life & Health reinsurance treaties often remain active for decades.

During the lifetime of a treaty:

- Source systems change
- Reporting formats evolve
- New fields are introduced
- Historical documentation becomes incomplete
- Data quality issues accumulate
- Historical adjustments are submitted

As a result, the same client may report data using multiple schemas throughout the life of a treaty while still expecting consistent operational and financial processing.

Historically, much of this work is performed manually through spreadsheets, emails, and institutional knowledge.

As reporting volumes increase, this approach becomes:

- Difficult to scale
- Error-prone
- Difficult to audit
- Difficult to govern

This platform was created to automate and standardize that process.

---

## End-to-End Architecture

```text
Client Premium Files
Client Claims Files
Premium Adjustments
Claim Adjustments
        │
        ▼
Data Ingestion Pipeline
        │
        ▼
Landing Zone (MinIO)
        │
        ▼
Bronze Layer (Delta Lake)
        │
        ▼
Schema Drift Detection
        │
        ▼
Human-in-the-Loop Mapping Review
        │
        ▼
Silver Staging Layer
        │
        ├── PII Masking
        ├── Data Quality Validation
        ├── Canonical Mapping
        └── Quarantine Processing
        │
        ▼
Silver Validation Layer
        │
        ├── Treaty Validation
        ├── Duplicate Detection
        ├── CAL Validation
        ├── Business Rule Validation
        └── 30% DQ Threshold Enforcement
        │
        ▼
SOA Workflow & Approval Process
        │
        ▼
SOA Reconciliation Layer
        │
        ├── Premium Reconciliation
        ├── Claims Reconciliation
        ├── Variance Analysis
        └── Financial Controls
        │
        ▼
Gold layer - Analytics Ready Data

```

---

## Core Features

### Data Ingestion

- Automated file ingestion
- File metadata extraction
- Hash-based idempotency framework
- Duplicate file detection
- Incremental processing
- MinIO object storage integration

### Schema Governance

- Schema drift detection
- Metadata-driven column mapping
- Human-in-the-Loop review workflow
- Mapping review queue
- Operational approval process
- Audit trail generation

### Silver Staging Framework

- Canonical schema enforcement
- Data type validation
- Business rule validation
- Data quality checks
- PII masking using SHA-256 hashing
- Quarantine routing for invalid records

### Silver Validation Framework

- Treaty mapping validation
- Benefit mapping validation
- Duplicate detection
- Historical hash validation
- CAL validation
- Retention and exposure checks
- Threshold-based file rejection

### Financial Controls

- Statement of Account (SOA) workflow
- SOA approval process
- Premium reconciliation
- Claims reconciliation
- Variance analysis
- Financial balancing controls

### Operational Control Plane

- Flask-based management UI
- Mapping review dashboard
- Validation monitoring
- SOA workflow management
- Operational alerting
- Automated email notifications

---

## Technology Stack

| Layer | Technology |
|---------|------------|
| Workflow Orchestration | Apache Airflow |
| Distributed Processing | Apache Spark |
| Storage Format | Delta Lake |
| Object Storage | MinIO |
| Metadata Store | PostgreSQL |
| Operational UI | Flask |
| Infrastructure | Docker |
| Programming Language | Python |

---

## Documentation

For a deeper understanding of the platform, refer to the following documents:

### 📖 Business Problem & Solution

- Reinsurance operational challenges
- Premium and claims validation lifecycle
- Historical adjustments
- SOA reconciliation process
- Operational workflows

👉 [View Document](./docs/business-problem.md)

### 🏗️ Technical Architecture

- End-to-end system architecture
- Design decisions and trade-offs
- Spark, Delta Lake, Airflow, PostgreSQL, MinIO, and Flask architecture
- Metadata-driven processing framework
- Schema governance and validation workflows

👉 [View Document](./docs/technical-architecture.md)

---

## Why This Architecture?

### Why Spark?

Reinsurance processing involves:

- Historical adjustments
- Cross-period validation
- Claims-to-premium matching
- Treaty exposure calculations
- Large-scale aggregations and joins

Apache Spark provides distributed processing capabilities that allow these operations to scale beyond traditional spreadsheet-based workflows.

### Why Delta Lake?

Reinsurance data frequently requires corrections and retrospective adjustments.

Delta Lake provides:

- ACID transactions
- Merge operations
- Schema evolution
- Auditability

which are critical for safely managing historical changes.

### Why Metadata-Driven Processing?

Every client reports differently.

Mappings, treaty configurations, validation rules, and operational controls are maintained as metadata rather than embedded directly into processing code.

This enables the platform to adapt to changing reporting requirements without requiring significant code changes.

---

## Repository Structure

```text
.
├── dags/
│   ├── data_ingestion_pipeline.py
│   ├── bronze_ingestion_pipeline.py
│   ├── silver_staging_dag.py
│   ├── silver_validation_dag.py
│   ├── recon_dag.py
│   ├── reminder_dag.py
│   └── SOA_Notification_DAG.py
│
├── dags/scripts/
│   ├── bronze_ingest.py
│   ├── silver_staging.py
│   ├── silver_validation.py
│   └── recon.py
│
├── mapping_ui/
│   └── app.py
│
├── docs/
│   ├── business-problem.md
│   ├── technical-architecture.md
│   └── architecture.png
│
├── docker-compose.yaml
├── Dockerfile.airflow
├── Dockerfile.flask
├── init.sql
├── requirements.txt
└── README.md
```

---

## Local Setup

### Clone Repository

```bash
git clone https://github.com/<username>/reinsurance-premium-claims-validation-platform.git

cd reinsurance-premium-claims-validation-platform
```

### Configure Environment

Create a local `.env` file using the provided environment template.

### Start Platform

```bash
docker compose up --build
```

### Access Services

| Service | URL |
|----------|------|
| Airflow | http://localhost:8080 |
| Flask UI | http://localhost:5000 |
| MinIO Console | http://localhost:9001 |

---

## Engineering Patterns Demonstrated

- Medallion Architecture
- Metadata-Driven Processing
- Delta Lake ACID Storage
- Schema Evolution Management
- Human-in-the-Loop Workflows
- Data Quality Frameworks
- Quarantine Processing
- Distributed Computing
- Operational Control Plane Design
- Financial Reconciliation Controls
- Reinsurance Data Governance

---

## Future Enhancements

- Power BI Reporting Layer
- Great Expectations Integration
- OpenMetadata / Data Lineage
- CI/CD Pipelines
- Cloud Deployment (AWS / Azure)
- Real-Time Streaming Ingestion
- LLM-Assisted Validation Insights

---

## Author

**Harshith Jain**

Technical Account Analyst – Life & Health Reinsurance | Data Engineering | Apache Spark | Airflow | Delta Lake | Data Governance | Reinsurance Data Management