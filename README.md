# Hệ thống Gợi ý Sản phẩm Thời gian Thực cho E-commerce

## Tổng quan Dự án

Hệ thống recommendation real-time được thiết kế để xử lý và phân tích hành vi người dùng trên website thương mại điện tử, từ đó tạo ra các gợi ý sản phẩm phù hợp trong thời gian thực. Dự án sử dụng kiến trúc streaming data với Apache Spark, Kafka, và Redis để đảm bảo độ trễ thấp và khả năng mở rộng cao.

## Cơ sở Lý thuyết về Apache Kafka

### Kafka là gì?

Apache Kafka là một nền tảng streaming phân tán (distributed event streaming platform) được thiết kế để xử lý các luồng dữ liệu real-time với khả năng mở rộng cao và độ tin cậy cao. Kafka hoạt động như một "message broker" - trung gian truyền tải dữ liệu giữa các ứng dụng.

### Kiến trúc cốt lõi của Kafka

**1. Broker (Kafka Server)**
- Là các server trong Kafka cluster
- Lưu trữ và phục vụ dữ liệu
- Mỗi broker có một ID duy nhất
- Có thể có nhiều broker để tăng khả năng chịu tải

**2. Topic (Chủ đề)**
- Là các danh mục dữ liệu được phân chia theo chủ đề
- Giống như các "kênh" để phân loại message
- Ví dụ: `user_events`, `recommendations`, `system_logs`

**3. Partition (Phân vùng)**
- Mỗi topic được chia thành nhiều partition
- Cho phép xử lý song song (parallel processing)
- Dữ liệu trong partition được sắp xếp theo thứ tự
- Mỗi message có một offset (vị trí) duy nhất

**4. Producer (Nhà sản xuất)**
- Ứng dụng gửi dữ liệu vào Kafka
- Quyết định message sẽ đi vào partition nào
- Có thể gửi message đồng bộ hoặc bất đồng bộ

**5. Consumer (Người tiêu dùng)**
- Ứng dụng đọc dữ liệu từ Kafka
- Có thể đọc từ một hoặc nhiều topic
- Theo dõi offset để biết đã đọc đến đâu


## Cơ sở Lý thuyết về Apache Spark

### Spark là gì?

Apache Spark là một unified analytics engine (công cụ phân tích thống nhất) được thiết kế để xử lý dữ liệu lớn với tốc độ cao. Spark có thể xử lý cả batch data (dữ liệu tĩnh) và streaming data (dữ liệu streaming) trong cùng một framework.

### Kiến trúc Spark Cluster

**1. Driver Program (Chương trình điều khiển)**
- Là chương trình chính chứa hàm `main()`
- Tạo SparkContext để kết nối với cluster
- Quản lý và điều phối các job
- Chứa logic của application

**2. Cluster Manager (Quản lý cluster)**
- Quản lý tài nguyên của cluster
- Phân bổ tài nguyên cho applications
- Các loại: Standalone, YARN, Kubernetes, Mesos

**3. Worker Nodes (Nút làm việc)**
- Các máy tính trong cluster
- Chạy Executor processes
- Cung cấp CPU và memory cho computation

**4. Executors (Bộ thực thi)**
- Processes chạy trên worker nodes
- Thực hiện các task được giao bởi driver
- Lưu trữ dữ liệu trong memory cho caching

## Tác dụng của từng Component trong Hệ thống

### 1. Producer Component

**Vị trí**: `producer/`
**Tệp chính**: `producer.py`

**Tác dụng:**
- **Data Generator**: Tạo ra dữ liệu giả lập hành vi người dùng
- **Event Publisher**: Gửi events vào Kafka topic `user_events`
- **Load Testing**: Mô phỏng traffic thực tế để test hệ thống

### 2. Spark-Apps Component

**Vị trí**: `spark-apps/`
**Tệp chính**: `simple_spark_engine.py`

**Tác dụng:**
- **Stream Processing Engine**: Xử lý dữ liệu streaming từ Kafka
- **Recommendation Generator**: Tạo ra gợi ý sản phẩm real-time
- **ML Inference**: Áp dụng các model machine learning

### 3. Model-Loader Component

**Vị trí**: `model-loader/`
**Tệp chính**: `model_loader.py`

**Tác dụng:**
- **Data Preprocessing**: Tiền xử lý dữ liệu training
- **Model Training**: Huấn luyện các ML models
- **Model Deployment**: Deploy models vào Redis để inference

### 4. Consumer Component (Dashboard)

**Vị trí**: `consumer/`
**Tệp chính**: `dashboard.py`

**Tác dụng:**
- **Visualization Interface**: Hiển thị kết quả recommendations
- **Real-time Monitoring**: Theo dõi hệ thống real-time
- **System Analytics**: Phân tích performance và metrics

## Sơ đồ Luồng Hệ thống Chi tiết

```
                    REAL-TIME E-COMMERCE RECOMMENDATION SYSTEM
                                              
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                                   SYSTEM ARCHITECTURE                                       │
└─────────────────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────┐    ┌─────────────────────┐    ┌─────────────────────┐    ┌─────────────────┐
│   PRODUCER      │    │    KAFKA CLUSTER    │    │   SPARK CLUSTER     │    │     REDIS       │
│                 │    │                     │    │                     │    │                 │
│ ┌─────────────┐ │    │ ┌─────────────────┐ │    │ ┌─────────────────┐ │    │ ┌─────────────┐ │
│ │Event        │ │────│ │  user_events    │ │────│ │ Spark Master    │ │────│ │ Similarity  │ │
│ │Generator    │ │    │ │     Topic       │ │    │ │   (Driver)      │ │    │ │   Matrix    │ │
│ └─────────────┘ │    │ └─────────────────┘ │    │ └─────────────────┘ │    │ └─────────────┘ │
│                 │    │                     │    │         │           │    │                 │
│ - view events   │    │ ┌─────────────────┐ │    │         ▼           │    │ ┌─────────────┐ │
│ - purchase      │    │ │recommendations  │ │    │ ┌─────────────────┐ │    │ │ FBT Rules   │ │
│ - add_to_cart   │    │ │     Topic       │ │    │ │ Spark Worker    │ │    │ │             │ │
│ - wishlist      │    │ └─────────────────┘ │    │ │  (Executors)    │ │    │ └─────────────┘ │
└─────────────────┘    └─────────────────────┘    │ └─────────────────┘ │    │                 │
         │                       │                │                     │    │ ┌─────────────┐ │
         │                       |                └─────────────────────┘    │ │ Product     │ │
         │                       │                         │                 │ │ Metadata    │ │
         ▼                       ▼                         ▼                 │ └─────────────┘ │
┌─────────────────────────────────────────────────────────────────────────┐  └─────────────────┘
│                           MODEL LOADER                                  │          │
│                                                                         │          │
│ ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐  │          │
│ │ Data        │  │ Similarity  │  │ FBT         │  │ Redis           │  │          │
│ │ Processing  │──│ Computing   │──│ Analysis    │──│ Deployment      │  │──────────┘
│ └─────────────┘  └─────────────┘  └─────────────┘  └─────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
                                         │
                                         ▼
                                ┌─────────────────┐
                                │   CONSUMER      │
                                │   (Dashboard)   │
                                │                 │
                                │ ┌─────────────┐ │
                                │ │ Real-time   │ │
                                │ │ Monitoring  │ │
                                │ └─────────────┘ │
                                │                 │
                                │ ┌─────────────┐ │
                                │ │ Web         │ │
                                │ │ Interface   │ │
                                │ └─────────────┘ │
                                │                 │
                                │   Port: 5000    │
                                └─────────────────┘

LUỒNG DỮ LIỆU CHI TIẾT:

1. PRODUCER → KAFKA: Generate user events → user_events topic
2. KAFKA → SPARK: Stream consumption → Real-time processing  
3. SPARK → REDIS: Query ML models → Feature extraction
4. SPARK → KAFKA: Publish results → recommendations topic
5. KAFKA → CONSUMER: Stream recommendations → Dashboard display
```

## Hướng dẫn Chạy Hệ thống từ Build đến Giao diện

### Bước 1: Dọn dẹp và Khởi động Infrastructure

**Dọn dẹp hệ thống cũ (nếu có):**
```bash
# Dừng tất cả containers
docker-compose down

# Xóa volumes để tránh xung đột (nếu cần)
docker-compose down -v
```

**Build và khởi động infrastructure services:**
```bash
# Build tất cả services
docker-compose build

# Khởi động infrastructure services
docker-compose up -d zookeeper kafka redis spark-master spark-worker model-loader
```

### Bước 2: Khởi động Data Pipeline

**Start Producer (Event Generator):**
```bash
# Khởi động producer để tạo user events
docker-compose up -d producer

# Kiểm tra log producer
docker-compose logs -f producer
```

**Start Spark Recommendation Engine:**
```bash
# Chạy Spark engine trong background
docker-compose exec -d spark-master python3 /opt/spark-apps/simple_spark_engine.py
```

### Bước 3: Khởi động Dashboard Interface

**Start Consumer Dashboard:**
```bash
# Khởi động dashboard
docker-compose up -d consumer

# Kiểm tra log dashboard
docker-compose logs -f consumer
```

### Bước 4: Truy cập Giao diện

**Mở trình duyệt và truy cập:**

1. **Main Dashboard**: http://localhost:5000
   - Real-time recommendations
   - System metrics  
   - User activity monitoring
   - Product analytics

2. **Spark Master UI**: http://localhost:8080
   - Cluster status
   - Running applications
   - Worker nodes info

3. **Spark Worker UI**: http://localhost:8081
   - Worker status
   - Executor information
   - Resource utilization