import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
import logging
import os
import uuid

# Thiết lập logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ClickEventGenerator:
    def __init__(self, bootstrap_servers='localhost:9092', topic='user_events'):
        """Khởi tạo producer với cấu hình Kafka"""
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8'),
            retries=5,
            retry_backoff_ms=300,
            request_timeout_ms=60000
        )
        
        # Dữ liệu mẫu
        self.categories = [
            'Electronics', 'Fashion', 'Home & Garden', 'Sports', 'Books',
            'Beauty', 'Automotive', 'Toys', 'Health', 'Food & Grocery'
        ]
        
        self.product_names = {
            'Electronics': ['iPhone 15', 'Samsung TV 55"', 'MacBook Pro', 'PlayStation 5', 'AirPods Pro'],
            'Fashion': ['Nike Sneakers', 'Levi Jeans', 'Adidas T-Shirt', 'Zara Dress', 'H&M Jacket'],
            'Home & Garden': ['IKEA Sofa', 'Garden Tools Set', 'Kitchen Mixer', 'LED Lamp', 'Vacuum Cleaner'],
            'Sports': ['Tennis Racket', 'Football', 'Yoga Mat', 'Running Shoes', 'Gym Equipment'],
            'Books': ['Python Programming', 'Data Science Book', 'Fiction Novel', 'Biography', 'Self-Help']
        }
        
        # User behavior patterns
        self.user_sessions = {}  # Theo dõi session của từng user
        
    def generate_product_data(self):
        """Tạo dữ liệu sản phẩm ngẫu nhiên"""
        category = random.choice(self.categories)
        product_names = self.product_names.get(category, ['Generic Product'])
        
        return {
            'product_id': f"P{random.randint(1, 500):04d}",
            'product_name': random.choice(product_names),
            'category': category,
            'price': round(random.uniform(10, 1000), 2),
        }
    
    def generate_user_session(self, user_id):
        """Tạo hoặc cập nhật session cho user"""
        if user_id not in self.user_sessions:
            self.user_sessions[user_id] = {
                'session_id': str(uuid.uuid4()),
                'start_time': datetime.now(),
                'products_viewed': [],
                'last_activity': datetime.now()
            }
        
        session = self.user_sessions[user_id]
        
        # Tạo session mới nếu user không hoạt động quá 30 phút
        if datetime.now() - session['last_activity'] > timedelta(minutes=30):
            self.user_sessions[user_id] = {
                'session_id': str(uuid.uuid4()),
                'start_time': datetime.now(),
                'products_viewed': [],
                'last_activity': datetime.now()
            }
            session = self.user_sessions[user_id]
        
        return session
    
    def generate_realistic_click_event(self):
        """Tạo click event thực tế với pattern hành vi người dùng"""
        user_id = f"U{random.randint(1, 100):04d}"
        session = self.generate_user_session(user_id)
        
        # Tạo product data
        product_data = self.generate_product_data()
        
        # Các loại actions
        actions = ['view', 'add_to_cart', 'purchase', 'add_to_wishlist']
        action_weights = [0.7, 0.15, 0.1, 0.05]  # View chiếm đa số
        
        # Nếu user đã xem sản phẩm trước đó, tăng khả năng action khác
        if session['products_viewed']:
            if random.random() < 0.3:  # 30% khả năng tương tác với sản phẩm đã xem
                # Chọn sản phẩm từ lịch sử
                viewed_product = random.choice(session['products_viewed'])
                product_data['product_id'] = viewed_product['product_id']
                product_data['category'] = viewed_product['category']
                
                # Tăng khả năng add_to_cart hoặc purchase
                action_weights = [0.3, 0.4, 0.25, 0.05]
        
        action = random.choices(actions, weights=action_weights)[0]
        
        # Tạo event
        event = {
            'user_id': user_id,
            'session_id': session['session_id'],
            'product_id': product_data['product_id'],
            'product_name': product_data['product_name'],
            'category': product_data['category'],
            'price': product_data['price'],
            'action': action,
            'timestamp': datetime.now().isoformat(),
            'user_agent': 'WebApp/1.0',
            'ip_address': f"192.168.1.{random.randint(1, 255)}",
            'page_url': f"/product/{product_data['product_id']}"
        }
        
        # Cập nhật session
        if action == 'view':
            session['products_viewed'].append({
                'product_id': product_data['product_id'],
                'category': product_data['category'],
                'timestamp': datetime.now()
            })
            
            # Giới hạn lịch sử (giữ 20 sản phẩm gần nhất)
            if len(session['products_viewed']) > 20:
                session['products_viewed'] = session['products_viewed'][-20:]
        
        session['last_activity'] = datetime.now()
        
        return event
    
    def send_event(self, event):
        """Gửi event vào Kafka"""
        try:
            # Sử dụng user_id làm key để ensure events của cùng user vào cùng partition
            future = self.producer.send(
                self.topic, 
                key=event['user_id'],
                value=event
            )
            
            # Không chờ confirmation để tăng throughput
            return True
            
        except Exception as e:
            logger.error(f"Error sending event: {e}")
            return False
    
    def generate_batch_events(self, batch_size=10):
        """Tạo một batch events để mô phỏng traffic burst"""
        events = []
        for _ in range(batch_size):
            event = self.generate_realistic_click_event()
            events.append(event)
        return events
    
    def run_continuous_generation(self, events_per_second=5, duration_seconds=None):
        """Chạy liên tục tạo events"""
        logger.info(f"Starting event generation: {events_per_second} events/second")
        
        if duration_seconds:
            end_time = datetime.now() + timedelta(seconds=duration_seconds)
            logger.info(f"Will run for {duration_seconds} seconds")
        else:
            end_time = None
            logger.info("Running indefinitely (Ctrl+C to stop)")
        
        event_count = 0
        last_log_time = datetime.now()
        
        try:
            while True:
                if end_time and datetime.now() >= end_time:
                    break
                
                # Tạo burst events đôi khi để mô phỏng traffic thực tế
                if random.random() < 0.1:  # 10% khả năng tạo burst
                    batch_size = random.randint(5, 15)
                    events = self.generate_batch_events(batch_size)
                    
                    for event in events:
                        if self.send_event(event):
                            event_count += 1
                    
                    logger.info(f"Sent burst of {batch_size} events")
                else:
                    # Tạo event đơn lẻ
                    event = self.generate_realistic_click_event()
                    if self.send_event(event):
                        event_count += 1
                
                # Log thống kê mỗi 30 giây
                if datetime.now() - last_log_time > timedelta(seconds=30):
                    active_sessions = len(self.user_sessions)
                    logger.info(f"Stats: {event_count} events sent, {active_sessions} active sessions")
                    last_log_time = datetime.now()
                
                # Delay để control rate
                time.sleep(1.0 / events_per_second)
                
        except KeyboardInterrupt:
            logger.info("Stopping event generation...")
        finally:
            self.producer.flush()
            self.producer.close()
            logger.info(f"Total events sent: {event_count}")

def main():
    """Main function"""
    # Lấy cấu hình từ environment variables
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    topic = os.getenv('KAFKA_TOPIC', 'user_events')
    events_per_second = int(os.getenv('EVENTS_PER_SECOND', 5))
    
    logger.info(f"Configuration:")
    logger.info(f"  Kafka servers: {kafka_servers}")
    logger.info(f"  Topic: {topic}")
    logger.info(f"  Events per second: {events_per_second}")
    
    # Khởi tạo generator
    generator = ClickEventGenerator(
        bootstrap_servers=kafka_servers.split(','),
        topic=topic
    )
    
    # Chạy generation
    generator.run_continuous_generation(events_per_second=events_per_second)

if __name__ == "__main__":
    main()