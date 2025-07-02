# E-Commerce Lakehouse Architecture on AWS

A production-grade serverless lakehouse implementation for e-commerce transaction processing, built using AWS managed services including Glue, S3, Step Functions, Lambda, and Athena.

## Overview

This project demonstrates the implementation of a modern data lakehouse architecture that ingests, transforms, validates, and exposes e-commerce transaction data for analytical workloads. The solution leverages Delta Lake format to provide ACID compliance and schema enforcement while maintaining the cost-effectiveness and scalability of object storage.

## Business Objectives

The lakehouse architecture addresses critical business requirements for e-commerce data processing:

- **Data Reliability**: Ensures transactional consistency and prevents data corruption through ACID-compliant operations
- **Schema Enforcement**: Maintains data quality through validation rules and schema evolution capabilities  
- **Real-time Analytics**: Provides fresh data availability for downstream analytical workloads
- **Cost Optimization**: Leverages S3 object storage while delivering warehouse-like performance
- **Operational Efficiency**: Automates the entire ETL lifecycle with minimal manual intervention

## Architecture Components

### Core AWS Services

| Service | Purpose |
|---------|---------|
| Amazon S3 | Raw zone and processed zone storage with lifecycle management |
| AWS Glue + Apache Spark | Distributed ETL processing with Delta Lake integration |
| Delta Lake | ACID-compliant table format providing transaction guarantees |
| AWS Step Functions | Orchestration engine for ETL workflow management |
| AWS Glue Data Catalog | Centralized metadata repository for schema management |
| Amazon Athena | Serverless query engine for analytical workloads |
| AWS Lambda | Event-driven processing for file detection and validation |
| Amazon SNS | Notification service for failure alerting |

### Data Flow Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────────┐
│   Raw S3 Zone   │───▶│  Lambda Trigger  │───▶│  Step Functions     │
│   (CSV Files)   │    │  (File Detection)│    │  (Orchestration)    │
└─────────────────┘    └──────────────────┘    └─────────────────────┘
                                                          │
                        ┌─────────────────────────────────┼─────────────────┐
                        │                                 │                 │
                        ▼                                 ▼                 ▼
               ┌─────────────────┐              ┌─────────────────┐ ┌─────────────────┐
               │ Products ETL    │              │ Orders ETL      │ │ Order Items ETL │
               │ (Glue + Delta)  │              │ (Glue + Delta)  │ │ (Glue + Delta)  │
               └─────────────────┘              └─────────────────┘ └─────────────────┘
                        │                                 │                 │
                        └─────────────────────────────────┼─────────────────┘
                                                          │
                                                          ▼
                                                ┌─────────────────────┐
                                                │  Processed S3 Zone  │
                                                │   (Delta Tables)    │
                                                └─────────────────────┘
                                                          │
                                                          ▼
                                                ┌─────────────────────┐
                                                │ Glue Data Catalog   │
                                                │ + Athena Analytics  │
                                                └─────────────────────┘
```

## Data Schema Design

### Dataset Specifications

**Products Table**
- `product_id` (String, Primary Key)
- `department_id` (Integer) 
- `department` (String)
- `product_name` (String)

**Orders Table**  
- `order_num` (String)
- `order_id` (String, Primary Key)
- `user_id` (String)
- `order_timestamp` (Timestamp)
- `total_amount` (Decimal)
- `date` (Date, Partition Key)

**Order Items Table**
- `id` (String, Primary Key)
- `order_id` (String, Foreign Key)
- `user_id` (String)
- `days_since_prior_order` (Integer)
- `product_id` (String, Foreign Key)
- `add_to_cart_order` (Integer)
- `reordered` (Boolean)
- `order_timestamp` (Timestamp)
- `date` (Date, Partition Key)

## Data Quality Framework

### Validation Rules

The ETL pipeline enforces comprehensive data quality checks:

- **Primary Key Constraints**: No null values allowed for primary identifiers
- **Timestamp Validation**: Ensures timestamps are within acceptable ranges and properly formatted
- **Referential Integrity**: Validates foreign key relationships between orders and order items
- **Deduplication Logic**: Removes duplicate records across multiple file ingestions
- **Schema Compliance**: Enforces data types and nullable constraints

### Error Handling

- Invalid records are logged to dedicated error tables for analysis
- Processing failures trigger SNS notifications for immediate attention
- Retry mechanisms handle transient failures automatically

## Implementation Features

### Delta Lake Integration

- **ACID Transactions**: Ensures data consistency across concurrent operations
- **Schema Evolution**: Supports backward-compatible schema changes
- **Time Travel**: Enables historical data analysis and rollback capabilities
- **Optimized Storage**: Automatic file compaction and Z-ordering for query performance

### Orchestration Logic

The Step Functions state machine implements:

1. **File Detection**: Lambda monitors S3 events for new data arrivals
2. **Sequential Processing**: ETL jobs for different datasets
3. **Dependency Management**: Ensures proper sequencing of operations
4. **Failure Recovery**: Automatic retry with exponential backoff
5. **Archival Process**: Moves processed files to archive location
6. **Catalog Updates**: Refreshes metadata for immediate query availability

### Performance Optimization

- **Partitioning Strategy**: Date-based partitioning for efficient query pruning
- **File Format**: Parquet format with compression for optimal storage and query performance
- **Resource Scaling**: Dynamic allocation of Glue DPU based on data volume
- **Caching**: Leverages Spark caching for iterative operations

## Deployment and CI/CD

### GitHub Actions Pipeline

The automated deployment process includes:

- **Code Quality Checks**: Linting and formatting validation
- **Unit Testing**: Comprehensive test coverage for ETL logic
- **Integration Testing**: End-to-end pipeline validation
- **Infrastructure Deployment**: Automated provisioning of AWS resources
- **Configuration Management**: Environment-specific parameter handling

### Deployment Strategy

- **Branch Protection**: All changes require pull request reviews
- **Environment Promotion**: Staging validation before production deployment
- **Rollback Capability**: Quick reversion to previous stable versions
- **Monitoring Integration**: Automated alerts for deployment success/failure

## Monitoring and Observability

### Operational Metrics

- ETL job execution times and success rates
- Data quality violation counts and trends
- Storage utilization and cost optimization metrics
- Query performance and user access patterns

### Alerting Framework

- Real-time notifications for pipeline failures
- Anomaly detection for unusual data patterns
- Resource utilization threshold alerts

## Security and Compliance

### Data Protection

- **Access Control**: IAM-based permissions with least privilege principle
- **Audit Logging**: Comprehensive CloudTrail integration
- **Data Lineage**: Complete tracking of data transformation processes

### Compliance Features

- **Data Retention**: Configurable retention policies for regulatory compliance
- **Change Tracking**: Immutable transaction logs for audit requirements
- **Access Monitoring**: Detailed logging of all data access activities
- **Data Classification**: Automated tagging for sensitive data identification

## Getting Started

### Prerequisites

- AWS CLI configured with appropriate permissions
- Python 3.8+ with required dependencies
- GitHub repository with Actions enabled

### Quick Start

1. Clone the repository and configure environment variables
2. Deploy infrastructure using provided templates
3. Upload sample data to the raw S3 zone
4. Monitor Step Functions execution in AWS Console
5. Validate results using provided Athena queries


## License

This project is provided as reference implementation for educational and professional development purposes.