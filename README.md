# Ecommerce-Analytics
Advanced ecommerce analytics platform with data ingestion, processing, and ML capabilities

## System Architecture

```
┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐
│  Data Sources   │──▶│  Ingestion      │──▶│  Processing     │──▶│  Storage        │
└─────────────────┘   └─────────────────┘   └─────────────────┘   └─────────────────┘
                                                                          │
┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐          │
│  ML & Insights  │◀──│  API Layer      │◀──│  Analytics      │◀─────────┘
└─────────────────┘   └─────────────────┘   └─────────────────┘
        │                     │                     │
        └─────────────────────┼─────────────────────┘
                              ▼
                    ┌─────────────────┐
                    │  Visualization  │
                    └─────────────────┘
```

## Component Breakdown

### 1. Data Sources
- **UK Retail Dataset**: e-commerce transaction data
- **Simulated Events**: Real-time customer behavior events
- **API Events**: Direct events from applications

### 2. Ingestion Layer
- **Apache Kafka**: Event streaming platform for real-time data
- **Kafka Connect**: For importing data from external sources
- **Event Producers**: Custom code for generating/simulating events

### 3. Processing Layer
- **Stream Processing**:
  - **Apache Flink**: For real-time event processing
  - **Event enrichment**: Adding context to raw events
  - **Metrics calculation**: Real-time aggregations and metrics
- **Batch Processing**:
  - **Apache Airflow**: Workflow orchestration
  - **Apache Spark**: Large-scale data processing
  - **dbt**: Data transformation in the warehouse

### 4. Storage Layer
- **Data Lake**:
  - **S3**: Raw and processed data storage
  - **Partitioning**: Data organized by date, event type
- **Operational Data**:
  - **MongoDB**: Flexible schema for processed events
  - **Redis**: Caching layer for performance
- **Data Warehouse**:
  - **Snowflake**: SQL-based analytics engine
  - **Dimensional model**: Fact and dimension tables

### 5. Analytics Layer
- **Data Transformation**:
  - **dbt**: SQL-based transformations
- **Machine Learning**:
  - **Customer Segmentation**: Grouping customers by behavior
  - **Product Recommendations**: Collaborative filtering
  - **Anomaly Detection**: Identifying unusual patterns

### 6. API Layer
- **REST API**: Access to analytics and data
- **Authentication**: Secure access control
- **Caching**: Performance optimization
- **Documentation**: OpenAPI/Swagger

### 7. Visualization
- **Grafana**: Real-time dashboard

### 8. Infrastructure
- **Docker**: Containerization
- **Kubernetes**: Container orchestration
- **Terraform**: Infrastructure as code
- **Prometheus**: Monitoring
- **ELK Stack**: Logging

## Data Flow

graph TD;
    A[UK Retail CSV (Raw Data)] --> B[Data Loader (Python Script)];
    B --> C[Event JSON File (Processed Events)];
    C --> D[Event Simulator (Python)];
    D --> E[Kafka Topic: `uk-retail-raw`];
    E --> F[Flink Processor (Python/Java)];
    F --> G[MongoDB (Processed Collections)];
    G --> H[FastAPI (API Service)];
    H --> I[Airflow DAGs (ETL Jobs)];
    I --> J[Data Warehouse (Snowflake/PostgreSQL)];
    J --> K[dbt Models (SQL Transformations)];


### Real-time Flow
1. Customer events are generated 
2. Events are published to Kafka topics
3. Flink consumes events, enriches them, and calculates metrics
4. Processed events are stored in MongoDB
5. Real-time metrics are published back to Kafka
6. Dashboards display real-time metrics
7. APIs serve real-time data to applications

### Batch Flow
1. Raw events are archived to S3 data lake
2. Airflow orchestrates daily ETL jobs
3. Spark jobs process the data and perform transformations
4. Processed data is loaded into Snowflake
5. dbt transforms raw data into analytics-ready models
6. Reports and dashboards access warehouse data
7. ML models are trained on historical data

## Technology Stack

| Component | Technology |
|-----------|------------|
| **Event Streaming** | Apache Kafka |
| **Stream Processing** | Apache Flink |
| **Batch Processing** | Apache Spark |
| **Workflow Orchestration** | Apache Airflow |
| **Data Lake** | Amazon S3 |
| **Document Database** | MongoDB |
| **Cache** | Redis |
| **Data Warehouse** | Snowflake |
| **Data Transformation** | dbt |
| **API** | Spring Boot / FastAPI |
| **ML Framework** | scikit-learn |
| **Visualization** | Grafana, Metabase |
| **Infrastructure** | Docker, Kubernetes, Terraform |
| **Monitoring** | Prometheus, Grafana |
| **Logging** | ELK Stack |

## Deployment Architecture

```
                     ┌──────────────────────────────────────────┐
                     │              AWS Cloud                   │
                     │                                          │
┌─────────┐          │  ┌─────────┐      ┌─────────────┐        │
│         │          │  │         │      │             │        │
│ Client  │─────────────▶  API    │──────▶  App Tier   │        │
│ Apps    │          │  │ Gateway │      │ (EKS/ECS)   │        │
│         │          │  │         │      │             │        │
└─────────┘          │  └─────────┘      └──────┬──────┘        │
                     │                          │               │
                     │                          ▼               │
                     │  ┌─────────┐      ┌─────────────┐        │
                     │  │         │      │             │        │
                     │  │ S3 Data │◀─────▶  Data Tier  │        │
                     │  │  Lake   │      │ (MSK/EMR)   │        │
                     │  │         │      │             │        │
                     │  └─────────┘      └──────┬──────┘        │
                     │                          │               │
                     │                          ▼               │
                     │  ┌─────────┐      ┌─────────────┐        │
                     │  │         │      │             │        │
                     │  │ Snowflake◀─────▶ Analytics   │        │
                     │  │         │      │   Tier      │        │
                     │  └─────────┘      │             │        │
                     │                   └─────────────┘        │
                     │                                          │
                     └──────────────────────────────────────────┘
```

Dataset Link : https://www.kaggle.com/datasets/carrie1/ecommerce-data/data
