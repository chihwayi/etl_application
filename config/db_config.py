import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class DatabaseConfig:
    """Database configuration class that reads from environment variables"""
    
    SOURCE_CONFIG = {
        'host': os.getenv('SOURCE_DB_HOST', '127.0.0.1'),
        'port': int(os.getenv('SOURCE_DB_PORT', 3306)),
        'user': os.getenv('SOURCE_DB_USER', 'root'),
        'password': os.getenv('SOURCE_DB_PASSWORD', 'root'),
        'database': os.getenv('SOURCE_DB_NAME', 'ctc2data')
    }
    
    MRS_CONFIG = {
        'host': os.getenv('MRS_DB_HOST', '127.0.0.1'),
        'port': int(os.getenv('MRS_DB_PORT', 3306)),
        'user': os.getenv('MRS_DB_USER', 'root'),
        'password': os.getenv('MRS_DB_PASSWORD', 'root'),
        'database': os.getenv('MRS_DB_NAME', 'mrs')
    }
    
    FACILITY_CONFIG = {
        'host': os.getenv('FACILITY_DB_HOST', '127.0.0.1'),
        'port': int(os.getenv('FACILITY_DB_PORT', 3306)),
        'user': os.getenv('FACILITY_DB_USER', 'root'),
        'password': os.getenv('FACILITY_DB_PASSWORD', 'root'),
        'database': os.getenv('FACILITY_DB_NAME', 'facility')
    }
    
    CLIENT_CONFIG = {
        'host': os.getenv('CLIENT_DB_HOST', '127.0.0.1'),
        'port': int(os.getenv('CLIENT_DB_PORT', 3306)),
        'user': os.getenv('CLIENT_DB_USER', 'root'),
        'password': os.getenv('CLIENT_DB_PASSWORD', 'root'),
        'database': os.getenv('CLIENT_DB_NAME', 'client')
    }
    
    CONSULTATION_CONFIG = {
        'host': os.getenv('CONSULTATION_DB_HOST', '127.0.0.1'),
        'port': int(os.getenv('CONSULTATION_DB_PORT', 3306)),
        'user': os.getenv('CONSULTATION_DB_USER', 'root'),
        'password': os.getenv('CONSULTATION_DB_PASSWORD', 'root'),
        'database': os.getenv('CONSULTATION_DB_NAME', 'consultation')
    }
    
    REPORT_CONFIG = {
        'host': os.getenv('REPORT_DB_HOST', '127.0.0.1'),
        'port': int(os.getenv('REPORT_DB_PORT', 3306)),
        'user': os.getenv('REPORT_DB_USER', 'root'),
        'password': os.getenv('REPORT_DB_PASSWORD', 'root'),
        'database': os.getenv('REPORT_DB_NAME', 'report')
    }
    
    # Batch size for processing
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', 1000))