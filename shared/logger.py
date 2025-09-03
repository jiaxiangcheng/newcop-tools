#!/usr/bin/env python3
"""
Shared logging configuration utility

Provides centralized logging setup for all scripts in the project.
Automatically creates logs directory and configures file and console handlers.
"""

import os
import logging
import sys
import platform
from typing import Optional

# Windows console encoding fix
if platform.system() == 'Windows':
    import codecs
    
    class SafeStreamHandler(logging.StreamHandler):
        """Custom stream handler that safely handles Unicode characters on Windows"""
        
        def __init__(self, stream=None):
            super().__init__(stream)
            # Map emoji to text alternatives for Windows console
            self.emoji_map = {
                '✅': '[OK]',
                '❌': '[ERROR]', 
                '⚠️': '[WARNING]',
                '🔧': '[CONFIG]',
                '📅': '[SCHEDULE]',
                '🔄': '[SYNC]',
                '📦': '[PACKAGE]',
                '🚀': '[START]',
                '📊': '[STATS]',
                '⏹️': '[STOP]',
                '🛍️': '[SHOP]',
                '🎯': '[TARGET]',
                '💡': '[TIP]',
                '🗑️': '[DELETE]',
                '➕': '[ADD]',
                '📁': '[FOLDER]',
                '🏪': '[STORE]',
                '📋': '[LIST]',
                '🚪': '[EXIT]',
                '🔸': '[CHOICE]',
                '📥': '[INPUT]',
                '👋': '[BYE]',
                '💥': '[CRASH]',
                '↩️': '[RETURN]'
            }
        
        def emit(self, record):
            try:
                # First try normal emission
                super().emit(record)
            except UnicodeEncodeError:
                # Handle Unicode error by replacing emoji with text
                try:
                    msg = self.format(record)
                    
                    # Replace emoji with safe alternatives
                    for emoji, replacement in self.emoji_map.items():
                        msg = msg.replace(emoji, replacement)
                    
                    # Try to write the safe message
                    self.stream.write(msg + self.terminator)
                    self.stream.flush()
                    
                except Exception:
                    # Final fallback: force ASCII encoding
                    try:
                        safe_msg = self.format(record).encode('ascii', errors='replace').decode('ascii')
                        for emoji, replacement in self.emoji_map.items():
                            safe_msg = safe_msg.replace(emoji, replacement)
                        self.stream.write(safe_msg + self.terminator)
                        self.stream.flush()
                    except Exception:
                        # If all else fails, write a simple error message
                        self.stream.write('[LOG ERROR: Unicode encoding issue]' + self.terminator)
                        self.stream.flush()

def setup_logger(
    logger_name: str,
    log_file_name: str = None,
    log_level: int = logging.INFO,
    format_string: Optional[str] = None,
    console_only: bool = False
) -> logging.Logger:
    """
    Set up a logger with both file and console handlers
    
    Args:
        logger_name: Name of the logger
        log_file_name: Name of the log file (e.g., 'inventory_sync.log'), optional if console_only=True
        log_level: Logging level (default: logging.INFO)
        format_string: Custom format string (optional)
        console_only: If True, only create console handler (no file logging)
    
    Returns:
        Configured logger instance
    """
    # Set up log file path only if not console_only
    log_file_path = None
    if not console_only and log_file_name:
        # Get project root directory (3 levels up from shared/)
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        logs_dir = os.path.join(project_root, 'logs')
        
        # Create logs directory if it doesn't exist
        os.makedirs(logs_dir, exist_ok=True)
        
        # Set up log file path
        log_file_path = os.path.join(logs_dir, log_file_name)
    
    # Default format string
    if format_string is None:
        format_string = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # Create logger
    logger = logging.getLogger(logger_name)
    logger.setLevel(log_level)
    
    # Clear any existing handlers to avoid duplicates
    logger.handlers.clear()
    
    # Create formatter
    formatter = logging.Formatter(format_string)
    
    # Create console handler with cross-platform Unicode support
    if platform.system() == 'Windows':
        # Use custom safe handler for Windows
        console_handler = SafeStreamHandler(sys.stdout)
    else:
        # Use standard handler for macOS/Linux
        console_handler = logging.StreamHandler(sys.stdout)
        # Try to set UTF-8 encoding if supported
        if hasattr(console_handler.stream, 'reconfigure'):
            try:
                console_handler.stream.reconfigure(encoding='utf-8')
            except Exception:
                pass
    
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    
    # Add console handler to logger
    logger.addHandler(console_handler)
    
    # Create file handler only if not console_only
    if not console_only and log_file_path:
        file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger

def get_logger(logger_name: str) -> logging.Logger:
    """
    Get an existing logger instance
    
    Args:
        logger_name: Name of the logger to retrieve
    
    Returns:
        Logger instance
    """
    return logging.getLogger(logger_name)
