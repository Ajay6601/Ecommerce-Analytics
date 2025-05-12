# config/logging_config.py
import os
import logging.config
import logging
from datetime import datetime

# Create logs directory if it doesn't exist
logs_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')
os.makedirs(logs_dir, exist_ok=True)

# Get current date for log file naming
current_date = datetime.now().strftime('%Y-%m-%d')

# Basic logging configuration
LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        },
        'detailed': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(pathname)s:%(lineno)d - %(message)s'
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'INFO',
            'formatter': 'standard',
            'stream': 'ext://sys.stdout'
        },
        'file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'level': 'INFO',
            'formatter': 'detailed',
            'filename': os.path.join(logs_dir, f'{current_date}.log'),
            'maxBytes': 10485760,  # 10 MB
            'backupCount': 10,
            'encoding': 'utf8'
        },
        'error_file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'level': 'ERROR',
            'formatter': 'detailed',
            'filename': os.path.join(logs_dir, f'error_{current_date}.log'),
            'maxBytes': 10485760,  # 10 MB
            'backupCount': 10,
            'encoding': 'utf8'
        }
    },
    'loggers': {
        '': {  # root logger
            'handlers': ['console', 'file', 'error_file'],
            'level': 'INFO',
            'propagate': True
        },
        'data_ingestion': {
            'handlers': ['console', 'file', 'error_file'],
            'level': 'INFO',
            'propagate': False
        },
        'flink_processing': {
            'handlers': ['console', 'file', 'error_file'],
            'level': 'INFO',
            'propagate': False
        },
        'api_service': {
            'handlers': ['console', 'file', 'error_file'],
            'level': 'INFO',
            'propagate': False
        }
    }
}

def setup_logging():
    """Configure logging for the application"""
    logging.config.dictConfig(LOGGING_CONFIG)
    logging.info("Logging configured successfully")

def get_logger(name):
    """Get a logger with the specified name"""
    return logging.getLogger(name)