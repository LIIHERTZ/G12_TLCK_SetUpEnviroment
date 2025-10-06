from flask import Flask, render_template, jsonify
from kafka import KafkaConsumer
import json
import redis
import logging
import os
import threading
from datetime import datetime
from collections import defaultdict, deque
import time

# Thiết lập logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

class RecommendationDashboard:
    def __init__(self, kafka_servers, topics, redis_host, redis_port):
        self.kafka_servers = kafka_servers
        self.topics = topics if isinstance(topics, list) else [topics]
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        
        # Data storage
        self.recent_recommendations = deque(maxlen=100)  # Keep last 100 recommendations
        self.user_activity = defaultdict(list)  # Track user activity
        self.product_stats = defaultdict(int)  # Product view counts
        self.category_stats = defaultdict(int)  # Category stats
        
        # Thread control
        self.running = False
        self.consumer_thread = None
        
    def start_consumer(self):
        """Start Kafka consumer in separate thread"""
        self.running = True
        self.consumer_thread = threading.Thread(target=self._consume_messages)
        self.consumer_thread.daemon = True
        self.consumer_thread.start()
        logger.info("Started Kafka consumer thread")
    
    def stop_consumer(self):
        """Stop Kafka consumer"""
        self.running = False
        if self.consumer_thread:
            self.consumer_thread.join(timeout=5)
        logger.info("Stopped Kafka consumer")
    
    def _consume_messages(self):
        """Consume messages from Kafka topics"""
        try:
            consumer = KafkaConsumer(
                *self.topics,
                bootstrap_servers=self.kafka_servers,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                group_id='recommendation_dashboard',
                auto_offset_reset='latest',
                enable_auto_commit=True
            )
            
            logger.info(f"Started consuming from topics: {self.topics}")
            
            for message in consumer:
                if not self.running:
                    break
                
                try:
                    data = message.value
                    topic = message.topic
                    
                    if topic == 'user_events':
                        self._process_click_event(data)
                    elif topic == 'recommendations':
                        self._process_recommendation(data)
                    
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
            
            consumer.close()
            
        except Exception as e:
            logger.error(f"Error in consumer: {e}")
    
    def _process_click_event(self, event):
        """Process user click events"""
        try:
            user_id = event.get('user_id')
            product_id = event.get('product_id')
            category = event.get('category')
            timestamp = event.get('timestamp', datetime.now().isoformat())
            
            # Update user activity
            self.user_activity[user_id].append({
                'product_id': product_id,
                'category': category,
                'timestamp': timestamp,
                'action': event.get('action', 'view')
            })
            
            # Keep only last 20 activities per user
            if len(self.user_activity[user_id]) > 20:
                self.user_activity[user_id] = self.user_activity[user_id][-20:]
            
            # Update stats
            self.product_stats[product_id] += 1
            if category:
                self.category_stats[category] += 1
                
        except Exception as e:
            logger.error(f"Error processing click event: {e}")
    
    def _process_recommendation(self, recommendation):
        """Process recommendation events"""
        try:
            # Add timestamp if not exists
            if 'received_at' not in recommendation:
                recommendation['received_at'] = datetime.now().isoformat()
            
            # Store in recent recommendations
            self.recent_recommendations.append(recommendation)
            
            logger.info(f"Received recommendation for user {recommendation.get('user_id')}")
            
        except Exception as e:
            logger.error(f"Error processing recommendation: {e}")
    
    def get_dashboard_data(self):
        """Get data for dashboard"""
        try:
            # Recent recommendations (last 20)
            recent_recs = list(self.recent_recommendations)[-20:]
            
            # Top products by views
            top_products = sorted(
                self.product_stats.items(), 
                key=lambda x: x[1], 
                reverse=True
            )[:10]
            
            # Category distribution
            category_dist = dict(self.category_stats)
            
            # Active users (users with activity in last hour)
            current_time = datetime.now()
            active_users = []
            
            for user_id, activities in self.user_activity.items():
                if activities:
                    last_activity = activities[-1]['timestamp']
                    try:
                        last_time = datetime.fromisoformat(last_activity.replace('Z', '+00:00'))
                        # Check if activity was within last hour
                        if (current_time - last_time.replace(tzinfo=None)).seconds < 3600:
                            active_users.append({
                                'user_id': user_id,
                                'last_activity': last_activity,
                                'total_activities': len(activities)
                            })
                    except:
                        pass
            
            # System stats
            stats = {
                'total_recommendations': len(self.recent_recommendations),
                'active_users': len(active_users),
                'total_products_viewed': sum(self.product_stats.values()),
                'unique_products': len(self.product_stats),
                'categories': len(self.category_stats)
            }
            
            return {
                'recent_recommendations': recent_recs,
                'top_products': top_products,
                'category_distribution': category_dist,
                'active_users': active_users[:10],  # Top 10 active users
                'stats': stats,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting dashboard data: {e}")
            return {'error': str(e)}
    
    def get_product_details(self, product_id):
        """Get product details from Redis"""
        try:
            product_key = f"product:{product_id}"
            product_data = self.redis_client.hgetall(product_key)
            
            if product_data:
                # Get similar products
                similar_key = f"similar:{product_id}"
                similar_products = self.redis_client.zrevrange(similar_key, 0, 4, withscores=True)
                
                # Get frequently bought together
                fbt_key = f"fbt:{product_id}"
                fbt_products = self.redis_client.zrevrange(fbt_key, 0, 4, withscores=True)
                
                return {
                    'product_data': product_data,
                    'similar_products': [{'product_id': pid, 'score': score} for pid, score in similar_products],
                    'fbt_products': [{'product_id': pid, 'score': score} for pid, score in fbt_products]
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting product details: {e}")
            return None

# Global dashboard instance
dashboard = None

@app.route('/')
def index():
    """Main dashboard page"""
    return render_template('dashboard.html')

@app.route('/api/dashboard')
def api_dashboard():
    """API endpoint for dashboard data"""
    if dashboard:
        return jsonify(dashboard.get_dashboard_data())
    return jsonify({'error': 'Dashboard not initialized'})

@app.route('/api/product/<product_id>')
def api_product(product_id):
    """API endpoint for product details"""
    if dashboard:
        product_data = dashboard.get_product_details(product_id)
        if product_data:
            return jsonify(product_data)
        return jsonify({'error': 'Product not found'}), 404
    return jsonify({'error': 'Dashboard not initialized'}), 500

@app.route('/health')
def health():
    """Health check endpoint"""
    return jsonify({'status': 'healthy', 'timestamp': datetime.now().isoformat()})

def create_html_template():
    """Create HTML template for dashboard"""
    template_dir = os.path.join(os.path.dirname(__file__), 'templates')
    os.makedirs(template_dir, exist_ok=True)
    
    html_content = '''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Real-time Product Recommendations Dashboard</title>
    <style>
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }
        .header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            border-radius: 10px;
            margin-bottom: 20px;
            text-align: center;
        }
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 20px;
        }
        .stat-card {
            background: white;
            padding: 20px;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            text-align: center;
        }
        .stat-card h3 {
            margin: 0 0 10px 0;
            color: #333;
            font-size: 2em;
        }
        .stat-card p {
            margin: 0;
            color: #666;
        }
        .dashboard-grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
        }
        .panel {
            background: white;
            border-radius: 10px;
            padding: 20px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        .panel h2 {
            margin-top: 0;
            color: #333;
            border-bottom: 2px solid #667eea;
            padding-bottom: 10px;
        }
        .recommendation-item {
            border: 1px solid #eee;
            border-radius: 5px;
            padding: 15px;
            margin-bottom: 10px;
            background: #f9f9f9;
        }
        .recommendation-header {
            font-weight: bold;
            color: #667eea;
            margin-bottom: 5px;
        }
        .product-list {
            display: flex;
            flex-wrap: wrap;
            gap: 5px;
        }
        .product-tag {
            background: #667eea;
            color: white;
            padding: 2px 8px;
            border-radius: 12px;
            font-size: 0.8em;
        }
        .top-products {
            list-style: none;
            padding: 0;
        }
        .top-products li {
            padding: 10px;
            border-bottom: 1px solid #eee;
            display: flex;
            justify-content: space-between;
        }
        .category-bar {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 8px 0;
            border-bottom: 1px solid #eee;
        }
        .category-count {
            background: #764ba2;
            color: white;
            padding: 2px 8px;
            border-radius: 12px;
            font-size: 0.8em;
        }
        .update-time {
            text-align: center;
            color: #666;
            font-size: 0.9em;
            margin-top: 20px;
        }
        @media (max-width: 768px) {
            .dashboard-grid {
                grid-template-columns: 1fr;
            }
        }
    </style>
</head>
<body>
    <div class="header">
        <h1>🛍️ Real-time Product Recommendations Dashboard</h1>
        <p>Monitoring user clicks and generating instant product recommendations</p>
    </div>

    <div class="stats-grid" id="stats-grid">
        <!-- Stats will be loaded here -->
    </div>

    <div class="dashboard-grid">
        <div class="panel">
            <h2>📊 Recent Recommendations</h2>
            <div id="recent-recommendations">
                Loading recommendations...
            </div>
        </div>

        <div class="panel">
            <h2>🏆 Top Products</h2>
            <ul id="top-products" class="top-products">
                Loading top products...
            </ul>
        </div>

        <div class="panel">
            <h2>📈 Category Distribution</h2>
            <div id="category-distribution">
                Loading categories...
            </div>
        </div>

        <div class="panel">
            <h2>👥 Active Users</h2>
            <div id="active-users">
                Loading active users...
            </div>
        </div>
    </div>

    <div class="update-time" id="update-time">
        Loading...
    </div>

    <script>
        async function fetchDashboardData() {
            try {
                const response = await fetch('/api/dashboard');
                const data = await response.json();
                
                if (data.error) {
                    console.error('Dashboard error:', data.error);
                    return;
                }
                
                updateStats(data.stats);
                updateRecommendations(data.recent_recommendations);
                updateTopProducts(data.top_products);
                updateCategoryDistribution(data.category_distribution);
                updateActiveUsers(data.active_users);
                updateTimestamp(data.timestamp);
                
            } catch (error) {
                console.error('Error fetching dashboard data:', error);
            }
        }

        function updateStats(stats) {
            const statsGrid = document.getElementById('stats-grid');
            statsGrid.innerHTML = `
                <div class="stat-card">
                    <h3>${stats.total_recommendations || 0}</h3>
                    <p>Total Recommendations</p>
                </div>
                <div class="stat-card">
                    <h3>${stats.active_users || 0}</h3>
                    <p>Active Users</p>
                </div>
                <div class="stat-card">
                    <h3>${stats.total_products_viewed || 0}</h3>
                    <p>Total Product Views</p>
                </div>
                <div class="stat-card">
                    <h3>${stats.unique_products || 0}</h3>
                    <p>Unique Products</p>
                </div>
                <div class="stat-card">
                    <h3>${stats.categories || 0}</h3>
                    <p>Categories</p>
                </div>
            `;
        }

        function updateRecommendations(recommendations) {
            const container = document.getElementById('recent-recommendations');
            
            if (!recommendations || recommendations.length === 0) {
                container.innerHTML = '<p>No recent recommendations</p>';
                return;
            }
            
            const html = recommendations.slice(-10).reverse().map(rec => `
                <div class="recommendation-item">
                    <div class="recommendation-header">
                        User: ${rec.user_id} | Trigger: ${rec.trigger_product}
                    </div>
                    <div>
                        <strong>Similar:</strong>
                        <div class="product-list">
                            ${(rec.similar_products || []).slice(0, 3).map(p => 
                                `<span class="product-tag">${p.product_id}</span>`
                            ).join('')}
                        </div>
                    </div>
                    <div>
                        <strong>FBT:</strong>
                        <div class="product-list">
                            ${(rec.frequently_bought_together || []).slice(0, 3).map(p => 
                                `<span class="product-tag">${p.product_id}</span>`
                            ).join('')}
                        </div>
                    </div>
                    <div style="font-size: 0.8em; color: #666; margin-top: 5px;">
                        ${new Date(rec.timestamp).toLocaleTimeString()}
                    </div>
                </div>
            `).join('');
            
            container.innerHTML = html;
        }

        function updateTopProducts(topProducts) {
            const container = document.getElementById('top-products');
            
            if (!topProducts || topProducts.length === 0) {
                container.innerHTML = '<li>No product data</li>';
                return;
            }
            
            const html = topProducts.slice(0, 10).map(([productId, count]) => `
                <li>
                    <span>${productId}</span>
                    <span>${count} views</span>
                </li>
            `).join('');
            
            container.innerHTML = html;
        }

        function updateCategoryDistribution(categories) {
            const container = document.getElementById('category-distribution');
            
            if (!categories || Object.keys(categories).length === 0) {
                container.innerHTML = '<p>No category data</p>';
                return;
            }
            
            const sortedCategories = Object.entries(categories)
                .sort(([,a], [,b]) => b - a);
            
            const html = sortedCategories.map(([category, count]) => `
                <div class="category-bar">
                    <span>${category}</span>
                    <span class="category-count">${count}</span>
                </div>
            `).join('');
            
            container.innerHTML = html;
        }

        function updateActiveUsers(users) {
            const container = document.getElementById('active-users');
            
            if (!users || users.length === 0) {
                container.innerHTML = '<p>No active users</p>';
                return;
            }
            
            const html = users.map(user => `
                <div style="padding: 10px; border-bottom: 1px solid #eee;">
                    <strong>${user.user_id}</strong><br>
                    <small>${user.total_activities} activities</small>
                </div>
            `).join('');
            
            container.innerHTML = html;
        }

        function updateTimestamp(timestamp) {
            const container = document.getElementById('update-time');
            container.textContent = `Last updated: ${new Date(timestamp).toLocaleString()}`;
        }

        // Initial load and periodic updates
        fetchDashboardData();
        setInterval(fetchDashboardData, 5000); // Update every 5 seconds
    </script>
</body>
</html>'''
    
    with open(os.path.join(template_dir, 'dashboard.html'), 'w', encoding='utf-8') as f:
        f.write(html_content)

def main():
    """Main function"""
    global dashboard
    
    # Configuration
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(',')
    kafka_topics = ['user_events', 'recommendations']
    redis_host = os.getenv('REDIS_HOST', 'localhost')
    redis_port = int(os.getenv('REDIS_PORT', 6379))
    
    logger.info("Starting Recommendation Dashboard...")
    logger.info(f"Kafka servers: {kafka_servers}")
    logger.info(f"Redis: {redis_host}:{redis_port}")
    
    # Create HTML template
    create_html_template()
    
    # Initialize dashboard
    dashboard = RecommendationDashboard(
        kafka_servers=kafka_servers,
        topics=kafka_topics,
        redis_host=redis_host,
        redis_port=redis_port
    )
    
    # Start consumer
    dashboard.start_consumer()
    
    try:
        # Run Flask app
        app.run(host='0.0.0.0', port=5000, debug=False)
    except KeyboardInterrupt:
        logger.info("Shutting down dashboard...")
    finally:
        dashboard.stop_consumer()

if __name__ == "__main__":
    main()