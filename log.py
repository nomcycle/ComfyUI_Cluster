import logging

def setup_logger(name='ComfyUI_Cluster'):
    """Set up and configure logger with consistent formatting"""
    logger = logging.getLogger(name)
    
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '[%(name)s] %(levelname)s: %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        # Prevent propagation to root logger
        logger.propagate = False
    
    return logger

# Create default logger instance
logger = setup_logger()