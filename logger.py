
import threading
import os
import logging
from logging.handlers import RotatingFileHandler

class ThreadSafeLogger:
    """线程安全的日志器"""
    
    @staticmethod
    def get_logger(name):
        """获取或创建线程专用日志器
        
        每个线程都会获得一个完全独立的日志器实例，
        确保不会与其他线程共享文件句柄
        """
        # 创建日志目录
        os.makedirs("logs", exist_ok=True)
        
        # 创建唯一的日志器名称 (使用线程ID确保唯一性)
        unique_name = f"{name}_{threading.get_ident()}"
        
        # 创建新的日志器实例
        logger = logging.getLogger(unique_name)
        
        # 如果已经配置过，直接返回
        if logger.handlers:
            return logger
            
        # 设置日志级别
        logger.setLevel(logging.INFO)
        
        # 创建文件处理器 - 每个线程使用独立的文件
        file_path = f"logs/{name}.log"
        file_handler = RotatingFileHandler(
            file_path, 
            maxBytes=1024*1024*1024,  # 1GB
            backupCount=5,
            encoding='utf-8'
        )
        
        # 设置格式
        formatter = logging.Formatter('%(asctime)s - [%(threadName)s] - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        
        # 添加处理器
        logger.addHandler(file_handler)
        
        # 确保不将日志传递给父日志器
        logger.propagate = False
        
        return logger
