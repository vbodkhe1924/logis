import psutil
import time
from confluent_kafka import Consumer
import json
import logging

class PipelineMonitor:
    def __init__(self):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
    
    def monitor_kafka_lag(self):
        """Monitor Kafka consumer lag"""
        self.consumer = Consumer({
        'bootstrap.servers': 'localhost:9092'
            })
        
        # Get lag metrics
        partitions = Consumer.assignment()
        for partition in partitions:
            committed = Consumer.committed(partition)
            last_offset = Consumer.end_offsets([partition])[partition]
            lag = last_offset - committed if committed else last_offset
            
            self.logger.info(f"Partition {partition.partition} lag: {lag}")
    
    def monitor_system_resources(self):
        """Monitor system resources"""
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        self.logger.info(f"CPU: {cpu_percent}%, Memory: {memory.percent}%, Disk: {disk.percent}%")
    
    def start_monitoring(self):
        """Start monitoring loop"""
        while True:
            try:
                self.monitor_system_resources()
                self.monitor_kafka_lag()
                time.sleep(60)  # Monitor every minute
            except Exception as e:
                self.logger.error(f"Monitoring error: {e}")
                time.sleep(10)

if __name__ == "__main__":
    monitor = PipelineMonitor()
    monitor.start_monitoring()