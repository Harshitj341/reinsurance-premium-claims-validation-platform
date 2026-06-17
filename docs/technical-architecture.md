# Reinsurance Premium & Claims Validation Platform

## Technical Architecture Overview

The Reinsurance Data Platform is a distributed data engineering system designed around a metadata-driven Medallion Architecture. The platform combines:
* Apache Airflow
* Apache Spark
* Delta Lake
* MinIO Object Storage
* PostgreSQL
* Flask
* Docker

to create a complete processing ecosystem for premium, claims, validation, and reconciliation workflows.

---

## Why This Architecture?

### Why Object Storage?
Premium and claims datasets are:
* Large
* Historical
* Append-heavy
* Frequently reprocessed

Traditional relational databases are not an efficient system of record for this type of workload. Object storage provides:
* Low-cost storage
* Virtually unlimited scalability
* Separation of storage and compute
* Native support for distributed processing

MinIO is used to provide an S3-compatible implementation for local deployment while maintaining compatibility with AWS S3 and cloud-native architectures.

### Why Apache Spark?
The platform performs operations such as:
* Treaty exposure calculations
* RISAR calculations
* Historical adjustments
* Claims-to-premium matching
* Cross-period reconciliation

These operations involve large joins, aggregations, and historical processing that become difficult to manage using spreadsheet-based workflows. Apache Spark provides:
* Distributed computation
* Parallel processing
* Scalable transformations
* Efficient aggregations
* Historical reprocessing capabilities

### Why Delta Lake?
Reinsurance data frequently requires adjustments and corrections. Clients may submit revised files months or years after original reporting. Delta Lake was selected because it provides:

* **ACID Transactions:** Guarantees consistent writes across distributed workloads.
* **Merge Operations:** Supports update, insert, and correction workflows through MERGE operations.
* **Schema Evolution:** Allows controlled adaptation to evolving client schemas.
* **Auditability:** Maintains historical table versions for investigation and review.

### Why PostgreSQL?
The platform separates business data from operational metadata. PostgreSQL stores:
* File tracking
* Validation results
* Schema mappings
* Treaty configurations
* Workflow state
* SOA records
* Reconciliation status

This creates a clear separation between:
$$\text{Operational State} \longrightarrow \text{PostgreSQL}$$
$$\text{Business Data} \longrightarrow \text{Delta Lake}$$

### Why Airflow?
The processing workflow contains strict dependencies. For example:
$$\text{Bronze Ingestion} \longrightarrow \text{Silver Standardization} \longrightarrow \text{Validation} \longrightarrow \text{Reconciliation}$$

Airflow provides:
* Dependency management
* Scheduling
* Retry handling
* Failure recovery
* Execution auditing

Each stage remains independently restartable and observable.

### Why Metadata-Driven Processing?
Every client reports differently. Hard-coding client-specific logic would create significant maintenance overhead. 

Instead, business rules are externalized into metadata tables. Examples include:
* Column mappings
* Treaty configurations
* Rate tables
* Validation rules

This allows platform behaviour to evolve without requiring major code changes.

---

## Data Architecture

The platform follows a Medallion Architecture:

Client Files
↓
Bronze Layer
↓
Silver Staging
↓
Validation Layer
↓
Silver Validated
↓
Gold Reconciliation Layer

### Bronze Layer
* **Purpose:** Raw immutable landing zone.
* **Responsibilities:**
  * File ingestion
  * File tracking
  * Duplicate detection
  * Audit logging

### Silver Staging Layer
* **Purpose:** Schema standardization and normalization.
* **Responsibilities:**
  * Dynamic column mapping
  * Type standardization
  * Schema drift detection
  * Metadata enrichment

### Silver Validation Layer
* **Purpose:** Business rule execution.
* **Responsibilities:**
  * Treaty validation
  * Premium validation
  * Claims validation
  * Rate validation
  * Cross-dataset validation
  * Quarantine processing

### Gold Layer
* **Purpose:** Business-ready datasets.
* **Responsibilities:**
  * Reconciliation
  * Financial reporting
  * Operational analytics
  * Treaty & segment level mappings (providing a booking convenience for the company)

*Note: Only validated records are promoted into this layer.*

---

## Operational Control Plane

A Flask application provides operational visibility into the platform. Features include:
* Schema mapping review
* Validation monitoring
* SOA workflow management
* Segregation of Duties controls
* Reconciliation monitoring

The application interacts directly with PostgreSQL and acts as the operational interface between business users and the processing platform.

---

## Engineering Patterns

The platform implements several common enterprise data engineering patterns:
* Medallion Architecture
* Metadata-Driven Processing
* Distributed Computing
* ACID Data Lake Storage
* Human-In-The-Loop Workflows
* Schema Evolution Management
* Data Quality Frameworks
* Quarantine Processing
* Operational Control Plane Design

Together these patterns create a scalable framework capable of supporting long-lived reinsurance reporting processes while adapting to changing client requirements.