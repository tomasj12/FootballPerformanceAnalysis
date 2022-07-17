from pyspark.sql import SparkSession
from utils import read_config, init_spark_session
import time
from datetime import datetime

def main():

    config = read_config()

    spark_app_name = config['spark_application']['spark_app_stream_name']

    file_format = config['streaming']['format']
    file_dir = config['streaming']['file_dir']

    agg_time = config['streaming_aggregation']['aggregating_time_s']
    wait_time = config['streaming_aggregation']['wait_for_agg_s']

    spark = init_spark_session(spark_app_name)

    delta = 0
    
    while delta < agg_time:
        
        now = datetime.now()
        time.sleep(wait_time)
        df = (
            spark.read.format(file_format).load(file_dir)
        )
        df_pd = df.toPandas()
        df_pd = df_pd.sort_values("wallClock")
        df_pd = df_pd.tail(1)
        alive = df_pd['ballStatus'].values[0]
        position = df_pd['ballInsideBox'].values[0]
        ts= df_pd['match_timestamp'].values[0]
        if alive != 'Alive':
            print(f'Timestamp: {ts}; Ball status: {alive}')
        
        if position:
            print(f'Timestamp: {ts}; Ball is inside penalty box!')

        then = datetime.now()
        delta = (then - now).seconds   
    print("Finisihed") 

if __name__ == '__main__':
    main()
        