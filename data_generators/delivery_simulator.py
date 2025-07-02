import json
import time
import random
from datetime import datetime, timedelta
from confluent_kafka import Producer
from faker import Faker
import threading

class LogisticsDataSimulator:
    def __init__(self):
        self.producer = Producer({
            'bootstrap.servers': 'localhost:9092',
            'client.id': 'logistics-simulator',
            'acks': 'all',  # Wait for all replicas to acknowledge
            'retries': 3,   # Retry failed sends
            'retry.backoff.ms': 1000
        })
        self.fake = Faker()
        self.drivers = self.generate_drivers(100)
        self.orders = self.generate_orders(1000)

    def delivery_callback(self, err, msg):
        """Callback for message delivery confirmation"""
        if err is not None:
            print(f'Message delivery failed: {err}')
        else:
            print(f'Message delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}')

    def send_message(self, topic, data):
        """Send message to Kafka topic with proper error handling"""
        try:
            # Convert data to JSON string
            message = json.dumps(data, default=str)
            
            # Use produce() method for confluent-kafka
            self.producer.produce(
                topic=topic,
                value=message,
                callback=self.delivery_callback
            )
            
            # Poll for delivery reports (non-blocking)
            self.producer.poll(0)
            
        except Exception as e:
            print(f"Failed to send message to {topic}: {e}")

    def generate_drivers(self, count):
        drivers = []
        for i in range(count):
            drivers.append({
                'driver_id': f'DRV_{i:04d}',
                'name': self.fake.name(),
                'phone': self.fake.phone_number(),
                'vehicle_type': random.choice(['bike', 'car', 'truck']),
                'rating': round(random.uniform(3.5, 5.0), 1),
                'status': 'available'
            })
        return drivers
    
    def generate_orders(self, count):
        orders = []
        for i in range(count):
            pickup_lat = 12.9716 + random.uniform(-0.1, 0.1)  # Bangalore coords
            pickup_lng = 77.5946 + random.uniform(-0.1, 0.1)
            dropoff_lat = pickup_lat + random.uniform(-0.05, 0.05)
            dropoff_lng = pickup_lng + random.uniform(-0.05, 0.05)
            
            orders.append({
                'order_id': f'ORD_{i:06d}',
                'customer_id': f'CUST_{random.randint(1, 10000):06d}',
                'pickup_location': {'lat': pickup_lat, 'lng': pickup_lng},
                'dropoff_location': {'lat': dropoff_lat, 'lng': dropoff_lng},
                'order_time': datetime.now() - timedelta(minutes=random.randint(0, 1440)),
                'status': 'pending',
                'priority': random.choice(['normal', 'high', 'urgent']),
                'estimated_distance': random.uniform(2, 25)
            })
        return orders
    
    def simulate_driver_location(self):
        """Simulate real-time driver location updates"""
        while True:
            try:
                for driver in random.sample(self.drivers, 20):  # 20 active drivers
                    location_data = {
                        'timestamp': datetime.now().isoformat(),
                        'driver_id': driver['driver_id'],
                        'latitude': 12.9716 + random.uniform(-0.1, 0.1),
                        'longitude': 77.5946 + random.uniform(-0.1, 0.1),
                        'speed': random.uniform(0, 60),  # km/h
                        'heading': random.uniform(0, 360),
                        'status': random.choice(['moving', 'idle', 'pickup', 'dropoff'])
                    }
                    
                    self.send_message('driver_locations', location_data)
                
                # Flush messages periodically
                self.producer.flush(timeout=1)
                time.sleep(2)  # Update every 2 seconds
                
            except Exception as e:
                print(f"Error in driver location simulation: {e}")
                time.sleep(5)  # Wait before retrying
    
    def simulate_delivery_status(self):
        """Simulate delivery status updates"""
        while True:
            try:
                order = random.choice(self.orders)
                status_data = {
                    'timestamp': datetime.now().isoformat(),
                    'order_id': order['order_id'],
                    'driver_id': random.choice(self.drivers)['driver_id'],
                    'status': random.choice(['assigned', 'picked_up', 'in_transit', 'delivered', 'cancelled']),
                    'location': {
                        'lat': 12.9716 + random.uniform(-0.1, 0.1),
                        'lng': 77.5946 + random.uniform(-0.1, 0.1)
                    },
                    'estimated_arrival': (datetime.now() + timedelta(minutes=random.randint(5, 60))).isoformat()
                }
                
                # Simulate delivery delays (5% chance)
                if random.random() < 0.05:
                    status_data['delay_reason'] = random.choice(['traffic', 'weather', 'vehicle_breakdown', 'customer_unavailable'])
                    status_data['delay_minutes'] = random.randint(10, 120)
                
                self.send_message('delivery_status', status_data)
                time.sleep(3)
                
            except Exception as e:
                print(f"Error in delivery status simulation: {e}")
                time.sleep(5)
    
    def simulate_traffic_conditions(self):
        """Simulate traffic condition updates"""
        while True:
            try:
                traffic_data = {
                    'timestamp': datetime.now().isoformat(),
                    'area_id': f'AREA_{random.randint(1, 50):03d}',
                    'traffic_density': random.choice(['low', 'medium', 'high', 'very_high']),
                    'average_speed': random.uniform(10, 50),
                    'incidents': random.randint(0, 3),
                    'weather_condition': random.choice(['clear', 'rain', 'fog', 'storm'])
                }
                
                self.send_message('traffic_conditions', traffic_data)
                time.sleep(10)
                
            except Exception as e:
                print(f"Error in traffic conditions simulation: {e}")
                time.sleep(10)
    
    def start_simulation(self):
        """Start all data simulation threads"""
        threads = [
            threading.Thread(target=self.simulate_driver_location),
            threading.Thread(target=self.simulate_delivery_status),
            threading.Thread(target=self.simulate_traffic_conditions)
        ]
        
        for thread in threads:
            thread.daemon = True
            thread.start()
        
        print("Data simulation started...")
        print("Topics: driver_locations, delivery_status, traffic_conditions")
        return threads

    def cleanup(self):
        """Clean up producer resources"""
        print("Flushing remaining messages...")
        self.producer.flush(timeout=10)
        print("Simulation cleanup complete")

if __name__ == "__main__":
    simulator = LogisticsDataSimulator()
    
    try:
        simulator.start_simulation()
        
        print("Simulation running... Press Ctrl+C to stop")
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\nStopping simulation...")
        simulator.cleanup()
        print("Simulation stopped")