import os
import sys
import logging
import argparse
from datetime import datetime

# Add the project directory to the path so we can import modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config.db_config import DatabaseConfig
from utils.logging_utils import setup_logging
from etl.manager import ETLManager


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='ETL Application for Healthcare Data Migration')
    
    parser.add_argument('--log-level', type=str, default='INFO',
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        help='Logging level')
    
    parser.add_argument('--log-file', type=str, default='logs/etl.log',
                        help='Path to log file')
    
    parser.add_argument('--batch-size', type=int, default=DatabaseConfig.BATCH_SIZE,
                        help='Number of records to process in each batch')
    
    parser.add_argument('--skip-client', action='store_true',
                        help='Skip client database migration')
    
    parser.add_argument('--skip-consultation', action='store_true',
                        help='Skip consultation database migration')
    
    parser.add_argument('--skip-report', action='store_true',
                        help='Skip report database migration')
    
    return parser.parse_args()


def main():
    """Main entry point for the ETL application"""
    # Parse command line arguments
    args = parse_arguments()
    
    # Setup logging
    start_time = datetime.now()
    logger = setup_logging(args.log_level, args.log_file)
    logger.info(f"Starting ETL application at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Create ETL manager
        etl_manager = ETLManager(batch_size=args.batch_size)
        
        # Run selected migrations
        etl_manager.run_migrations(
            run_client=not args.skip_client,
            run_consultation=not args.skip_consultation,
            run_report=not args.skip_report
        )
        
        # Log statistics
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info(f"ETL application completed in {duration:.2f} seconds")
        logger.info(f"Migration statistics: {etl_manager.get_stats()}")
        
        return 0
        
    except Exception as e:
        logger.error(f"Error in ETL application: {str(e)}")
        return 1


if __name__ == "__main__":
    sys.exit(main())