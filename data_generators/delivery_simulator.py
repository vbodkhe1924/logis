import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
from faker import Faker
import threading

class LogisticsDataSimulator:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.fake = Faker()
        self.drivers = self.generate_drivers(100)
        self.orders = self.generate_orders(1000)
        
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
                
                self.producer.send('driver_locations', location_data)
            
            time.sleep(2)  # Update every 2 seconds
    
    def simulate_delivery_status(self):
        """Simulate delivery status updates"""
        while True:
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
            
            self.producer.send('delivery_status', status_data)
            time.sleep(3)
    
    def simulate_traffic_conditions(self):
        """Simulate traffic condition updates"""
        while True:
            traffic_data = {
                'timestamp': datetime.now().isoformat(),
                'area_id': f'AREA_{random.randint(1, 50):03d}',
                'traffic_density': random.choice(['low', 'medium', 'high', 'very_high']),
                'average_speed': random.uniform(10, 50),
                'incidents': random.randint(0, 3),
                'weather_condition': random.choice(['clear', 'rain', 'fog', 'storm'])
            }
            
            self.producer.send('traffic_conditions', traffic_data)
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
        return threads

if __name__ == "__main__":
    simulator = LogisticsDataSimulator()
    simulator.start_simulation()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Simulation stopped")