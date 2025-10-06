#!/usr/bin/env python3

import json
import redis
import logging
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer

# Thiết lập logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class RecommendationEngine:
    def __init__(self, redis_host="redis", redis_port=6379):
        """Khởi tạo Recommendation Engine với Redis connection"""
        try:
            self.redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
            # Test connection
            self.redis_client.ping()
            logger.info(f"✅ Connected to Redis at {redis_host}:{redis_port}")
        except Exception as e:
            logger.error(f"❌ Failed to connect to Redis: {e}")
            raise
        
    def get_similar_products(self, product_id, limit=5):
        """Lấy danh sách sản phẩm tương tự từ Redis"""
        try:
            similar_key = f"similar:{product_id}"
            similar_products = self.redis_client.zrevrange(similar_key, 0, limit-1, withscores=True)
            return [{"product_id": pid, "score": float(score)} for pid, score in similar_products]
        except Exception as e:
            logger.error(f"Error getting similar products for {product_id}: {e}")
            return []
    
    def get_frequently_bought_together(self, product_id, limit=3):
        """Lấy danh sách sản phẩm thường mua kèm từ Redis"""
        try:
            fbt_key = f"fbt:{product_id}"
            fbt_products = self.redis_client.zrevrange(fbt_key, 0, limit-1, withscores=True)
            return [{"product_id": pid, "score": float(score)} for pid, score in fbt_products]
        except Exception as e:
            logger.error(f"Error getting FBT for {product_id}: {e}")
            return []

def create_kafka_consumer():
    """Tạo Kafka consumer"""
    return KafkaConsumer(
        'user_events',
        bootstrap_servers=['kafka:9092'],
        group_id='spark_recommendation_group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',
        enable_auto_commit=True
    )

def create_kafka_producer():
    """Tạo Kafka producer"""
    return KafkaProducer(
        bootstrap_servers=['kafka:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        retries=3,
        batch_size=16384,
        linger_ms=10
    )

def main():
    """Main function - Spark-style processing"""
    logger.info("🚀 Starting Spark-style Recommendation System...")
    
    # Khởi tạo components
    rec_engine = RecommendationEngine()
    consumer = create_kafka_consumer()
    producer = create_kafka_producer()
    
    event_count = 0
    
    try:
        logger.info("✅ Spark-style Recommendation system started successfully!")
        logger.info("📥 Listening for click events on Kafka topic 'user_events'")
        logger.info("📤 Sending recommendations to Kafka topic 'recommendations'")
        
        # Xử lý streaming events (giống như Spark Streaming)
        for message in consumer:
            try:
                event = message.value
                event_count += 1
                
                logger.info(f"📥 Processing event #{event_count}: User {event['user_id']} {event['action']} product {event['product_id']}")
                
                # Tạo recommendations
                similar_products = rec_engine.get_similar_products(event['product_id'])
                fbt_products = rec_engine.get_frequently_bought_together(event['product_id'])
                
                recommendation = {
                    'user_id': event['user_id'],
                    'session_id': event['session_id'],
                    'timestamp': datetime.now().isoformat(),
                    'trigger_product': event['product_id'],
                    'trigger_action': event['action'],
                    'similar_products': similar_products,
                    'frequently_bought_together': fbt_products,
                    'reason': f"Based on {event['action']} {event['product_id']}"
                }
                
                # Gửi recommendation vào Kafka topic
                producer.send('recommendations', value=recommendation)
                producer.flush()
                
                logger.info(f"✅ Sent recommendation: {len(similar_products)} similar + {len(fbt_products)} FBT products")
                
            except Exception as e:
                logger.error(f"❌ Error processing event: {e}")
                continue
                
    except KeyboardInterrupt:
        logger.info("🛑 Stopping recommendation system...")
    except Exception as e:
        logger.error(f"❌ Error in main: {e}")
        raise
    finally:
        try:
            consumer.close()
            producer.close()
            logger.info("✅ Kafka connections closed")
        except:
            pass

if __name__ == "__main__":
    main()