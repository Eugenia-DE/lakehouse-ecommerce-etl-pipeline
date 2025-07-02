# E-Commerce Lakehouse Data Pipeline - Technical Documentation

## Business Requirements

### Project Context

The e-commerce platform processes structured transactional data across multiple business domains including product catalog, customer orders, and order line items. The organization requires a robust lakehouse architecture that provides automated data ingestion, processing, cataloging, validation, and archival capabilities for datasets stored in Amazon S3. The solution must support downstream analytics and business intelligence workloads using Delta Lake format and AWS managed services.

### Functional Requirements

**Data Ingestion and Processing**
- Automatically detect and process new data files deposited in S3 raw zone
- Implement idempotent processing logic to prevent duplicate data ingestion
- Ensure atomic transactions and data consistency across all operations
- Maintain comprehensive audit trails for all processing activities

**Data Quality and Validation**
- Enforce schema validation and data quality rules during ingestion
- Implement deduplication logic to handle overlapping datasets
- Validate referential integrity between related datasets
- Generate detailed error logs for rejected records

**Data Storage and Cataloging**
- Store processed data in Delta Lake format for ACID compliance
- Maintain AWS Glue Data Catalog for metadata management
- Implement optimal partitioning strategies for query performance
- Support schema evolution and backward compatibility

**Operational Requirements**
- Validate data availability through automated Athena queries
- Archive successfully processed source files with retention policies
- Generate processing markers to track completion status
- Implement comprehensive error handling and notification mechanisms

### Target Stakeholders

**Data Engineering Teams**: Responsible for pipeline development, maintenance, and optimization
**Data Analytics Teams**: Consume processed datasets for business intelligence and reporting
**Business Users**: Rely on data freshness and accuracy for decision-making processes

## System Architecture

### Infrastructure Components

**Storage Layer**
- **Raw Zone** (`s3://bucket/raw/`): Ingestion point for source data files
- **Processed Zone** (`s3://bucket/processed/`): Delta Lake tables for analytical workloads
- **Archive Zone** (`s3://bucket/archived/`): Long-term storage for processed source files
- **Query Results** (`s3://bucket/athena-query-results/`): Athena query output storage

**Compute Services**
- **AWS Glue Jobs**: Distributed ETL processing using Apache Spark with Delta Lake integration
- **AWS Lambda Functions**: Serverless compute for lightweight processing tasks
- **AWS Step Functions**: Workflow orchestration and state management
- **Amazon Athena**: Serverless query engine for data validation and analytics

**Integration Services**
- **AWS Glue Data Catalog**: Centralized metadata repository and schema registry
- **Amazon SNS**: Notification service for operational alerts and error handling
- **AWS CloudWatch**: Monitoring and logging infrastructure

### Security and Access Control

**IAM Roles and Policies**

**Glue Service Role** (`AWSGlueLakehouseRole`)
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::lakehouse-bucket/*",
        "arn:aws:s3:::lakehouse-bucket"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase",
        "glue:GetTable",
        "glue:CreateTable",
        "glue:UpdateTable",
        "glue:GetPartitions"
      ],
      "Resource": "*"
    }
  ]
}
```

**Lambda Execution Role**
- Basic Lambda execution permissions
- S3 read/write access for marker file operations
- Glue service invocation capabilities
- Step Functions state machine execution permissions

**GitHub Actions OIDC Role**
- Deployment permissions for CI/CD pipeline
- Step Functions definition update capabilities
- CloudFormation stack management access

## ETL Pipeline Implementation

### Data Processing Jobs

**Product ETL Pipeline** (`glue_jobs/product_etl.py`)

The product ETL job processes product catalog data with the following operations:

1. **Data Validation**: Validates product_id uniqueness and required field completeness
2. **Schema Enforcement**: Applies predefined schema with appropriate data types
3. **Deduplication**: Removes duplicate product records based on product_id
4. **Delta Lake Integration**: Writes data using Delta format with ACID guarantees
5. **Catalog Registration**: Updates Glue Data Catalog with table metadata

```python
# Example validation logic
def validate_product_data(df):
    # Check for null product_ids
    null_count = df.filter(df.product_id.isNull()).count()
    if null_count > 0:
        logger.warning(f"Found {null_count} records with null product_id")
    
    # Remove duplicates based on product_id
    deduplicated_df = df.dropDuplicates(['product_id'])
    
    return deduplicated_df
```

**Orders ETL Pipeline** (`glue_jobs/orders_etl.py`)

The orders ETL job handles transactional order data:

1. **Timestamp Validation**: Ensures order timestamps are within acceptable ranges
2. **Business Rule Enforcement**: Validates total_amount is positive and non-null
3. **Partitioning Strategy**: Partitions data by order date for query optimization
4. **Incremental Processing**: Supports both full and incremental data loads

**Order Items ETL Pipeline** (`glue_jobs/order_items_etl.py`)

The order items ETL job processes line-item details:

1. **Referential Integrity**: Validates foreign key relationships with orders and products
2. **Data Enrichment**: Calculates derived fields such as order_item_total
3. **Anomaly Detection**: Identifies unusual patterns in ordering behavior
4. **Performance Optimization**: Implements broadcast joins for dimension lookups

### Lambda Functions

**Processed Marker Checker** (`lambda/check_processed_marker.py`)

This function implements idempotency by checking for existing processing markers:

```python
def lambda_handler(event, context):
    raw_key = event['raw_key']
    dataset_type = event['dataset_type']
    
    marker_key = f"processed/_processed_log/{dataset_type}/{raw_key.split('/')[-1]}.txt"
    
    try:
        s3_client.head_object(Bucket=BUCKET_NAME, Key=marker_key)
        return {"already_processed": True}
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return {"already_processed": False}
        else:
            raise
```

**Glue Crawler Initiator** (`lambda/start_glue_crawler.py`)

This function triggers the Glue crawler to update the Data Catalog:

```python
def lambda_handler(event, context):
    crawler_name = event['crawler_name']
    
    try:
        glue_client.start_crawler(Name=crawler_name)
        return {"crawler_started": True}
    except ClientError as e:
        if "CrawlerRunningException" in str(e):
            return {"crawler_already_running": True}
        else:
            raise
```

### Step Functions Orchestration

The Step Functions state machine (`lakehouse_etl_stepfunction.json`) orchestrates the entire ETL pipeline:

**State Machine Flow**
1. **Marker Validation**: Check if file has been previously processed
2. **Conditional Processing**: Execute ETL jobs only for unprocessed files
3. **Parallel Execution**: Process multiple datasets concurrently when possible
4. **Catalog Management**: Update Glue Data Catalog with new table metadata
5. **Data Validation**: Execute Athena queries to validate data quality
6. **Archival Process**: Move source files to archive location
7. **Marker Generation**: Create processing completion markers

**Error Handling Strategy**
- **Retry Logic**: Automatic retry with exponential backoff for transient failures
- **Failure Notifications**: SNS alerts for critical processing failures
- **Partial Failure Recovery**: Ability to resume from failed states
- **Timeout Management**: Configurable timeouts for long-running operations

## Continuous Integration and Deployment

### GitHub Actions Workflows

**Development Pipeline** (`.github/workflows/lakehouse-dev.yml`)

Executes on push to development branches:
- Code quality checks using flake8 and black
- Unit test execution with pytest
- Static analysis with mypy
- Security scanning with bandit

**Production Pipeline** (`.github/workflows/lakehouse-main.yml`)

Executes on merge to main branch:
- All development pipeline checks
- Integration test execution
- Infrastructure deployment via CloudFormation
- Step Functions definition deployment
- Smoke testing of deployed resources

**Deployment Strategy**
- **Branch Protection**: Requires pull request reviews and status checks
- **Environment Promotion**: Automated promotion from development to production
- **Rollback Capability**: Ability to quickly revert to previous stable versions
- **Blue-Green Deployment**: Zero-downtime deployments for critical components

## Operations and Monitoring

### Data Processing Workflow

**File Ingestion Process**
1. Data files are deposited in the appropriate S3 raw zone path
2. Optional Lambda trigger detects new file arrivals
3. Step Functions state machine is invoked with file metadata
4. Marker check determines if processing is required
5. Appropriate ETL jobs are executed based on file type and content

**Processing Validation**
1. Glue crawler updates Data Catalog with new table schemas
2. Athena validation queries confirm data accessibility
3. Data quality metrics are collected and stored
4. Processing completion markers are generated

**Archival and Cleanup**
1. Successfully processed source files are moved to archive location
2. Temporary processing artifacts are cleaned up
3. Processing logs are updated with completion status
4. Monitoring metrics are published to CloudWatch

### Troubleshooting Guide

**Common Processing Issues**

**Issue**: Glue Job Fails with NoSuchKey Error
- **Root Cause**: Specified S3 object key does not exist or is inaccessible
- **Resolution Steps**:
  1. Verify file exists in expected S3 location
  2. Check IAM permissions for Glue service role
  3. Validate S3 key format matches expected pattern
  4. Review CloudWatch logs for detailed error information

**Issue**: Duplicate Processing Prevention
- **Root Cause**: Processing marker already exists for the specified file
- **Resolution Options**:
  - Delete existing marker file to force reprocessing
  - Upload source file with different name or timestamp
  - Review processing logs to confirm previous execution status

**Issue**: Zero Records Written to Delta Table
- **Root Cause**: All source records fail validation or deduplication logic
- **Resolution Steps**:
  1. Review source data quality and format compliance
  2. Check validation logic for overly restrictive rules
  3. Examine foreign key relationships and reference data
  4. Analyze CloudWatch logs for specific validation failures

**Issue**: Step Functions Execution Timeout
- **Root Cause**: Long-running operations exceed configured timeout limits
- **Resolution Steps**:
  1. Review Step Functions execution history in AWS Console
  2. Identify bottleneck states causing delays
  3. Optimize resource allocation for Glue jobs
  4. Adjust timeout configurations based on data volume patterns

**Issue**: Athena Query Validation Failures
- **Root Cause**: Data Catalog not updated or query syntax errors
- **Resolution Steps**:
  1. Verify Glue crawler completed successfully
  2. Check table registration in Glue Data Catalog
  3. Validate Athena query syntax and table references
  4. Review partitioning and projection configurations

### Performance Optimization

**Glue Job Tuning**
- **DPU Allocation**: Right-size data processing units based on data volume
- **Parallelism**: Configure optimal number of executors and cores
- **Memory Management**: Tune executor memory and overhead settings
- **Caching Strategy**: Implement appropriate DataFrame caching for iterative operations

**Delta Lake Optimization**
- **File Compaction**: Schedule regular OPTIMIZE operations
- **Z-Ordering**: Implement Z-order indexing for frequently queried columns
- **Vacuum Operations**: Clean up unused files to reduce storage costs
- **Table Properties**: Configure appropriate table properties for workload patterns

**Query Performance**
- **Partitioning Strategy**: Optimize partition pruning for common query patterns
- **Projection Configuration**: Implement partition projection for time-based queries
- **Columnar Storage**: Leverage Parquet format benefits for analytical workloads
- **Compression**: Use appropriate compression codecs for storage efficiency

## Project Structure and Development

### Repository Organization

```
ecommerce-lakehouse/
├── glue_jobs/                          # ETL job implementations
│   ├── product_etl.py                  # Product catalog processing
│   ├── orders_etl.py                   # Order transaction processing
│   ├── order_items_etl.py              # Order line item processing
│   └── archive_and_mark_processed.py   # File archival and marker generation
├── lambda/                             # Lambda function implementations
│   ├── check_processed_marker.py       # Idempotency marker validation
│   └── start_glue_crawler.py           # Glue crawler initiation
├── .github/workflows/                  # CI/CD pipeline definitions
│   ├── lakehouse-dev.yml               # Development workflow
│   └── lakehouse-main.yml              # Production deployment workflow
├── tests/                              # Test suites
│   ├── unit/                           # Unit tests
│   ├── integration/                    # Integration tests
│   └── fixtures/                       # Test data fixtures
├── docs/                               # Documentation
│   ├── architecture/                   # Architecture diagrams
│   └── runbooks/                       # Operational procedures
├── lakehouse_etl_stepfunction.json     # Step Functions definition
├── requirements.txt                    # Python dependencies
├── README.md                           # Project overview
└── CHANGELOG.md                        # Version history
```

### Development Best Practices

**Code Quality Standards**
- **PEP 8 Compliance**: Consistent code formatting and style
- **Type Annotations**: Comprehensive type hints for better maintainability
- **Documentation**: Docstrings for all functions and classes
- **Error Handling**: Comprehensive exception handling with appropriate logging

**Testing Strategy**
- **Unit Testing**: Isolated testing of individual components
- **Integration Testing**: End-to-end pipeline validation
- **Performance Testing**: Load testing with realistic data volumes
- **Security Testing**: Vulnerability scanning and penetration testing

**Operational Considerations**
- **Idempotency**: All operations are designed to be safely repeatable
- **Monitoring**: Comprehensive logging and metrics collection
- **Alerting**: Proactive notification of operational issues
- **Scalability**: Designed to handle growing data volumes and complexity

This lakehouse implementation provides a robust foundation for e-commerce data processing with enterprise-grade reliability, scalability, and maintainability features.