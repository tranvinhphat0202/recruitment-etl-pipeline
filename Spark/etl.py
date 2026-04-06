# recruitment_etl.py
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
import datetime
import time

# -----------------------------
# 1) Config
# -----------------------------
CASSANDRA_HOST = "cassandra"
CASSANDRA_KEYSPACE = "recruitment_dl"
CASSANDRA_TABLE = "tracking_raw_csv"

MYSQL_HOST = "recruitment_mysql"
MYSQL_PORT = "3306"
MYSQL_DB = "recruitment_dw"
MYSQL_USER = "root"
MYSQL_PASS = "root123"
MYSQL_TABLE = "events"

MYSQL_URL = (
    f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
    f"?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC"
)

MYSQL_PROPS = {
    "user": MYSQL_USER,
    "password": MYSQL_PASS,
    "driver": "com.mysql.cj.jdbc.Driver",
}

# -----------------------------
# 2) Spark Session
# -----------------------------
spark = SparkSession.builder \
    .appName("Recruitment-ETL-Step-by-Step") \
    .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.1.0") \
    .getOrCreate()

# -----------------------------
# 3) Các hàm xử lý (Transformations)
# -----------------------------

def calculating_aggregates(df, event_type):
    # Aggregation
    sub_df = df.filter(df.custom_track == event_type).na.fill(0)
    sub_df.createOrReplaceTempView(f"tmp_{event_type}")
    
    if event_type == 'click':
        return spark.sql(f"""
            SELECT job_id, date(ts) as dates, hour(ts) as hours, publisher_id, campaign_id, group_id, 
                   avg(bid) as bid_set, count(*) as clicks, sum(bid) as spend_hour 
            FROM tmp_{event_type}
            GROUP BY job_id, dates, hours, publisher_id, campaign_id, group_id
        """)
    else:
        col_name = 'conversion' if event_type == 'conversion' else f'{event_type}_application'
        return spark.sql(f"""
            SELECT job_id, date(ts) as dates, hour(ts) as hours, publisher_id, campaign_id, group_id, 
                   count(*) as {col_name}
            FROM tmp_{event_type}
            GROUP BY job_id, dates, hours, publisher_id, campaign_id, group_id
        """)

def retrieve_company_data():
    # Truy vấn bảng job từ MySQL
    sql = "(SELECT job_id, company_id, group_id, campaign_id FROM job) test"
    company = spark.read.jdbc(url=MYSQL_URL, table=sql, properties=MYSQL_PROPS)
    return company

def import_to_mysql(output, cass_max_time):
    # 1. Chọn đúng các cột hiện có trong DataFrame sau khi Join
    final_output = output.select(
        'job_id', 
        'dates', 
        'hours', 
        'publisher_id', 
        'company_id', 
        'campaign_id', 
        'group_id',
        'unqualified_application', 
        'qualified_application',   
        'conversion',              
        'clicks', 
        'bid_set', 
        'spend_hour'
    ).withColumn('updated_at', sf.lit(cass_max_time).cast("timestamp"))

    # Fill '0' xử lý null value
    final_output = final_output.na.fill({
        'job_id': '0',
        'publisher_id': '0',
        'campaign_id': '0',
        'group_id': '0',
        'company_id': '0'
    }).na.fill(0)
    
    # 2. Thêm cột sources
    final_output = final_output.withColumn('sources', sf.lit('Cassandra'))
    
    # 3. In schema để kiểm tra lần cuối trước khi ghi
    print("--- Final Schema to be written to MySQL ---")
    final_output.printSchema()
    
    # 4. Ghi vào MySQL
    final_output.write.format("jdbc") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("url", MYSQL_URL) \
        .option("dbtable", MYSQL_TABLE) \
        .mode("append") \
        .option("user", MYSQL_USER) \
        .option("password", MYSQL_PASS) \
        .save()
        
    return print('Data imported successfully to MySQL events table')
    
    

# -----------------------------
# 4) Main Task 
# -----------------------------

def main_task(mysql_time, cass_max_time):
    print('The host is ', MYSQL_HOST)
    print('The port using is ', MYSQL_PORT)
    print('The db using is ', MYSQL_DB)
    
    print('-----------------------------')
    print('Retrieving data from Cassandra')
    print('-----------------------------')
    df = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table=CASSANDRA_TABLE, keyspace=CASSANDRA_KEYSPACE).load() \
        .where(sf.col('ts') > mysql_time)

    print('-----------------------------')
    print('Selecting data from Cassandra')
    print('-----------------------------')
    df = df.select('ts', 'job_id', 'custom_track', 'bid', 'campaign_id', 'group_id', 'publisher_id')
    df = df.filter(df.job_id.isNotNull())
    df.printSchema()

    print('-----------------------------')
    print('Processing Cassandra Output')
    print('-----------------------------')
    # Thực hiện aggregate cho 4 loại event
    clicks = calculating_aggregates(df, 'click')
    conversions = calculating_aggregates(df, 'conversion')
    qualified = calculating_aggregates(df, 'qualified')
    unqualified = calculating_aggregates(df, 'unqualified')
    
    # Full join các bảng
    join_keys = ['job_id', 'dates', 'hours', 'publisher_id', 'campaign_id', 'group_id']
    cassandra_output = clicks.join(conversions, join_keys, 'full') \
                             .join(qualified, join_keys, 'full') \
                             .join(unqualified, join_keys, 'full')

    print('-----------------------------')
    print('Merge Company Data')
    print('-----------------------------')
    company = retrieve_company_data()

    print('-----------------------------')
    print('Finalizing Output')
    print('-----------------------------')
    # Join lấy company_id và xử lý các cột trùng
    final_output = cassandra_output.join(company, 'job_id', 'left') \
                                   .drop(company.group_id).drop(company.campaign_id) \
                                   .na.fill(0)

    print('-----------------------------')
    print('Import Output to MySQL')
    print('-----------------------------')
    import_to_mysql(final_output, cass_max_time)
    
    print('Task Finished')

# -----------------------------
# 5) Vòng lặp Incremental
# -----------------------------

def get_mysql_latest_time():
    try:
        # Lấy mốc thời gian sự kiện chính xác nhất thay vì chỉ lấy Ngày
        sql = f"(SELECT max(updated_at) FROM {MYSQL_TABLE}) as data"
        df = spark.read.jdbc(url=MYSQL_URL, table=sql, properties=MYSQL_PROPS)
        res = df.collect()[0][0]
        return str(res) if res else '1998-01-01 23:59:59'
    except:
        return '1998-01-01 23:59:59'

if __name__ == "__main__":
    current_checkpoint = get_mysql_latest_time()
    print(f"Starting ETL. Current MySQL checkpoint: {current_checkpoint}")

    while True:
        start_now = datetime.datetime.now()
        
        # 1. Lấy mốc lớn nhất hiện tại trong Cassandra
        data_cass = spark.read.format("org.apache.spark.sql.cassandra") \
                         .options(table=CASSANDRA_TABLE, keyspace=CASSANDRA_KEYSPACE).load()
        # Lấy giá trị Max TS thực tế
        res_cass = data_cass.agg({'ts': 'max'}).collect()[0][0]
        cassandra_latest = str(res_cass) if res_cass else current_checkpoint

        print(f"Checking: Cassandra ({cassandra_latest}) vs Checkpoint ({current_checkpoint})")

        # 2. So sánh: Chỉ chạy nếu Cassandra có dữ liệu MỚI HƠN checkpoint
        if cassandra_latest > current_checkpoint:
            print(f"--- New Data Found! Processing from {current_checkpoint} ---")

            # Chạy task xử lý case when 
            main_task(current_checkpoint, cassandra_latest)

            # Quan Trọng !!! --> Cập nhật checkpoint bằng mốc vừa lấy được từ Cassandra
            current_checkpoint = cassandra_latest
        else:
            print("No new data found. Sleeping...")
        execution_time = (datetime.datetime.now() - start_now).total_seconds()
        print(f"Job finished in {execution_time}s. Next scan in 5s.")
        time.sleep(5)