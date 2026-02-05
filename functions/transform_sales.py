#!/usr/bin/env python3
"""
Transform Sales Function (Demo Stub)
Demonstrates the transformation step of the ETL pipeline.

Functions receive arguments from the pipeline YAML definition.
Environment variables are still available for secrets and global config.
"""

import os
import json
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main(input_table: str = "staging_raw_transactions", output_table: str = "transformed_sales"):
    """
    Main entry point for the transformation function.

    Args:
        input_table: The staging table to read from
        output_table: The destination table to write to
    """
    logger.info("Starting sales transformation (demo mode)")

    # Get system config from environment (set by the platform)
    project_id = os.environ.get('PROJECT_ID', 'demo-project')
    execution_id = os.environ.get('EXECUTION_ID', 'local-run')
    pipeline_name = os.environ.get('PIPELINE_NAME', 'daily_etl')
    task_name = os.environ.get('TASK_NAME', 'transform')

    # Secrets/credentials still come from environment (via Secret Manager)
    dataset = os.environ.get('DATASET', 'sales_data')

    logger.info(f"Configuration:")
    logger.info(f"  Project ID: {project_id}")
    logger.info(f"  Dataset: {dataset}")
    logger.info(f"  Input Table: {input_table}")
    logger.info(f"  Output Table: {output_table}")
    logger.info(f"  Execution ID: {execution_id}")
    logger.info(f"  Pipeline: {pipeline_name}")
    logger.info(f"  Task: {task_name}")

    # Simulate transformation steps
    logger.info("Step 1: Loading data from staging table...")
    logger.info("Step 2: Calculating tax amounts...")
    logger.info("Step 3: Categorizing products...")
    logger.info("Step 4: Computing customer segments...")
    logger.info("Step 5: Validating output data...")
    logger.info("Step 6: Saving to destination table...")

    # Simulate some transformed records
    input_records = 1250
    output_records = 1248  # 2 records filtered out during validation

    result = {
        'status': 'success',
        'records_input': input_records,
        'records_output': output_records,
        'validation_passed': True,
        'duration_seconds': 12.8,
        'metrics': {
            'total_revenue': 125847.50,
            'total_tax': 8809.33,
            'unique_customers': 342,
            'bulk_orders': 28,
            'high_value_orders': 15
        },
        'execution_id': execution_id,
        'demo_mode': True
    }

    logger.info(f"Transformation completed successfully")
    logger.info(f"Records processed: {input_records} -> {output_records}")
    logger.info(f"Total revenue: ${result['metrics']['total_revenue']:,.2f}")

    print(json.dumps(result, indent=2, default=str))
    return result


if __name__ == "__main__":
    main()
