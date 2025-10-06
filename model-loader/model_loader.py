import redis
import json
import random
import logging
import os
import time
from datetime import datetime

# Thiết lập logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ProductModelLoader:
    def __init__(self, redis_host='redis', redis_port=6379):
        """Khởi tạo connection tới Redis"""
        self.redis_client = None
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.connect_to_redis()
        
    def connect_to_redis(self):
        """Kết nối tới Redis với retry logic"""
        max_retries = 30
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                self.redis_client = redis.Redis(
                    host=self.redis_host, 
                    port=self.redis_port, 
                    decode_responses=True,
                    socket_connect_timeout=5,
                    socket_timeout=5
                )
                
                # Test connection
                self.redis_client.ping()
                logger.info(f"Successfully connected to Redis at {self.redis_host}:{self.redis_port}")
                return
                
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1}/{max_retries} - Cannot connect to Redis: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    raise Exception(f"Failed to connect to Redis after {max_retries} attempts")
    
    def generate_product_categories(self):
        """Tạo danh sách categories và products"""
        categories = {
            'Electronics': {
                'products': [f'P{i:04d}' for i in range(1, 51)],  # P0001-P0050
                'price_range': (50, 2000)
            },
            'Fashion': {
                'products': [f'P{i:04d}' for i in range(51, 101)],  # P0051-P0100
                'price_range': (20, 500)
            },
            'Home & Garden': {
                'products': [f'P{i:04d}' for i in range(101, 151)],  # P0101-P0150
                'price_range': (30, 1000)
            },
            'Sports': {
                'products': [f'P{i:04d}' for i in range(151, 201)],  # P0151-P0200
                'price_range': (25, 800)
            },
            'Books': {
                'products': [f'P{i:04d}' for i in range(201, 251)],  # P0201-P0250
                'price_range': (10, 100)
            },
            'Beauty': {
                'products': [f'P{i:04d}' for i in range(251, 301)],  # P0251-P0300
                'price_range': (15, 300)
            },
            'Automotive': {
                'products': [f'P{i:04d}' for i in range(301, 351)],  # P0301-P0350
                'price_range': (50, 5000)
            },
            'Toys': {
                'products': [f'P{i:04d}' for i in range(351, 401)],  # P0351-P0400
                'price_range': (10, 200)
            },
            'Health': {
                'products': [f'P{i:04d}' for i in range(401, 451)],  # P0401-P0450
                'price_range': (20, 500)
            },
            'Food & Grocery': {
                'products': [f'P{i:04d}' for i in range(451, 501)],  # P0451-P0500
                'price_range': (5, 100)
            }
        }
        return categories
    
    def generate_similarity_scores(self, product_id, all_products, same_category_products):
        """Tạo similarity scores cho một product"""
        similarities = {}
        
        # Sản phẩm cùng category có similarity score cao hơn
        for prod in same_category_products:
            if prod != product_id:
                # Similarity trong cùng category: 0.3 - 0.9
                score = round(random.uniform(0.3, 0.9), 3)
                similarities[prod] = score
        
        # Thêm một số sản phẩm khác category với similarity thấp hơn
        other_products = [p for p in all_products if p not in same_category_products and p != product_id]
        num_other_similar = random.randint(3, 8)  # 3-8 sản phẩm khác category
        
        for prod in random.sample(other_products, min(num_other_similar, len(other_products))):
            # Similarity khác category: 0.1 - 0.4
            score = round(random.uniform(0.1, 0.4), 3)
            similarities[prod] = score
        
        return similarities
    
    def generate_fbt_scores(self, product_id, all_products, same_category_products):
        """Tạo Frequently Bought Together scores"""
        fbt_scores = {}
        
        # FBT với sản phẩm cùng category
        same_cat_fbt = random.sample(
            [p for p in same_category_products if p != product_id], 
            min(random.randint(2, 5), len(same_category_products) - 1)
        )
        
        for prod in same_cat_fbt:
            # FBT score: 0.2 - 0.8
            score = round(random.uniform(0.2, 0.8), 3)
            fbt_scores[prod] = score
        
        # FBT với sản phẩm khác category (complementary products)
        other_products = [p for p in all_products if p not in same_category_products and p != product_id]
        num_other_fbt = random.randint(1, 4)  # 1-4 sản phẩm khác category
        
        for prod in random.sample(other_products, min(num_other_fbt, len(other_products))):
            score = round(random.uniform(0.1, 0.5), 3)
            fbt_scores[prod] = score
        
        return fbt_scores
    
    def load_product_data(self):
        """Load tất cả product data vào Redis"""
        logger.info("Starting to load product data into Redis...")
        
        # Generate categories và products
        categories = self.generate_product_categories()
        all_products = []
        product_to_category = {}
        
        # Tạo mapping product -> category
        for category, data in categories.items():
            all_products.extend(data['products'])
            for product in data['products']:
                product_to_category[product] = category
        
        logger.info(f"Generated {len(all_products)} products across {len(categories)} categories")
        
        # Xóa dữ liệu cũ
        logger.info("Clearing old data...")
        for key in self.redis_client.scan_iter(match="similar:*"):
            self.redis_client.delete(key)
        for key in self.redis_client.scan_iter(match="fbt:*"):
            self.redis_client.delete(key)
        for key in self.redis_client.scan_iter(match="product:*"):
            self.redis_client.delete(key)
        
        # Load similarity data và FBT data
        loaded_count = 0
        
        for product_id in all_products:
            category = product_to_category[product_id]
            same_category_products = categories[category]['products']
            price_range = categories[category]['price_range']
            
            # Generate và store similarity scores
            similarities = self.generate_similarity_scores(
                product_id, all_products, same_category_products
            )
            
            # Store similarities as sorted set (higher score = more similar)
            similar_key = f"similar:{product_id}"
            if similarities:
                self.redis_client.zadd(similar_key, similarities)
            
            # Generate và store FBT scores
            fbt_scores = self.generate_fbt_scores(
                product_id, all_products, same_category_products
            )
            
            # Store FBT as sorted set
            fbt_key = f"fbt:{product_id}"
            if fbt_scores:
                self.redis_client.zadd(fbt_key, fbt_scores)
            
            # Store product metadata
            product_data = {
                'product_id': product_id,
                'category': category,
                'price': round(random.uniform(*price_range), 2),
                'created_at': datetime.now().isoformat()
            }
            
            product_key = f"product:{product_id}"
            self.redis_client.hset(product_key, mapping=product_data)
            
            loaded_count += 1
            
            if loaded_count % 50 == 0:
                logger.info(f"Loaded {loaded_count}/{len(all_products)} products...")
        
        # Store metadata
        metadata = {
            'total_products': str(len(all_products)),
            'categories': ','.join(list(categories.keys())),
            'loaded_at': datetime.now().isoformat(),
            'version': '1.0'
        }
        
        self.redis_client.hset("model:metadata", mapping=metadata)
        
        logger.info(f"Successfully loaded {loaded_count} products into Redis!")
        return loaded_count
    
    def load_category_mappings(self):
        """Load category mappings vào Redis"""
        logger.info("Loading category mappings...")
        
        categories = self.generate_product_categories()
        
        for category, data in categories.items():
            # Store products by category
            category_key = f"category:{category}"
            self.redis_client.delete(category_key)  # Clear old data
            
            for product in data['products']:
                self.redis_client.sadd(category_key, product)
        
        logger.info(f"Loaded {len(categories)} category mappings")
    
    def verify_data(self):
        """Verify data đã được load chính xác"""
        logger.info("Verifying loaded data...")
        
        # Check metadata
        metadata = self.redis_client.hgetall("model:metadata")
        if not metadata:
            logger.error("No metadata found!")
            return False
        
        total_products = int(metadata['total_products'])
        logger.info(f"Expected products: {total_products}")
        
        # Check some random products
        sample_products = [f"P{i:04d}" for i in random.sample(range(1, min(501, total_products + 1)), 5)]
        
        for product_id in sample_products:
            # Check similarity data
            similar_key = f"similar:{product_id}"
            similar_count = self.redis_client.zcard(similar_key)
            
            # Check FBT data
            fbt_key = f"fbt:{product_id}"
            fbt_count = self.redis_client.zcard(fbt_key)
            
            # Check product data
            product_key = f"product:{product_id}"
            product_data = self.redis_client.hgetall(product_key)
            
            logger.info(f"Product {product_id}: {similar_count} similar, {fbt_count} FBT, metadata: {bool(product_data)}")
        
        logger.info("Data verification completed!")
        return True
    
    def get_stats(self):
        """Lấy thống kê về dữ liệu đã load"""
        stats = {}
        
        # Metadata
        metadata = self.redis_client.hgetall("model:metadata")
        stats['metadata'] = metadata
        
        # Count keys by pattern
        stats['similar_keys'] = len(list(self.redis_client.scan_iter(match="similar:*")))
        stats['fbt_keys'] = len(list(self.redis_client.scan_iter(match="fbt:*")))
        stats['product_keys'] = len(list(self.redis_client.scan_iter(match="product:*")))
        stats['category_keys'] = len(list(self.redis_client.scan_iter(match="category:*")))
        
        return stats

def main():
    """Main function"""
    redis_host = os.getenv('REDIS_HOST', 'redis')
    redis_port = int(os.getenv('REDIS_PORT', 6379))
    
    logger.info(f"Starting model loader...")
    logger.info(f"Redis connection: {redis_host}:{redis_port}")
    
    try:
        # Khởi tạo loader
        loader = ProductModelLoader(redis_host=redis_host, redis_port=redis_port)
        
        # Load data
        loader.load_product_data()
        loader.load_category_mappings()
        
        # Verify
        if loader.verify_data():
            logger.info("✅ Model loaded successfully!")
        else:
            logger.error("❌ Model verification failed!")
            return 1
        
        # Show stats
        stats = loader.get_stats()
        logger.info("📊 Final Statistics:")
        for key, value in stats.items():
            logger.info(f"  {key}: {value}")
        
        return 0
        
    except Exception as e:
        logger.error(f"Error loading model: {e}")
        return 1

if __name__ == "__main__":
    exit(main())