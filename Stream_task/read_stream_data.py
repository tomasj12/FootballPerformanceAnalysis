import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from utils import init_spark_session, read_config,ball_inside_box

def main():
    config = read_config()
    spark_app_name = config['spark_application']['spark_app_stream_name']
    print(config)
    checkpoint = config['streaming']['file_checkpoint']
    file_location = config['streaming']['file_dir']
    termination = config['streaming']['wait_for_termination']
    file_format = config['streaming']['format']
    
    spark = init_spark_session(spark_app_name)

    print("==========================================================")
    print("Streaming has started!")
    print(f"Results are saved in {format} format in dir: {file_location}")
    print("\n")

    df = (
        spark
        .readStream
        .format("socket")
        .option('host','localhost')
        .option('port','9898')
        .load()
    )

    split_df = (
        df
        .withColumn('wallClock',F.split("value",":").getItem(0))
        .withColumn('match_timestamp',F.from_unixtime(F.col('wallClock')/1000, 'yyyy-MM-dd HH:mm:ss:SSS'))
        .withColumn('playerAttributes',F.split("value",":").getItem(1))
        .withColumn('ballAttributes',F.split("value",":").getItem(2))
        .withColumn("ball_position_x",F.split("ballAttributes",",").getItem(0)/100)
        .withColumn("ball_position_y",F.split("ballAttributes",",").getItem(1)/100)
        .withColumn("ball_position_z",F.split("ballAttributes",",").getItem(2)/100)
        .withColumn("ballPosition",F.array('ball_position_x','ball_position_y','ball_position_z'))
        .withColumn("ballStatus",F.regexp_replace(F.split("ballAttributes",",").getItem(5),";",""))
        .withColumn("ballSpeed",F.split("ballAttributes",",").getItem(3))
        .withColumn("ballTeam",F.split("ballAttributes",",").getItem(4))
        .withColumn("ballInsideBox",ball_inside_box("ballPosition",F.lit("inside_box")))
        .withColumn("ballInsideField",ball_inside_box("ballPosition",F.lit("inside_field")))
        .select("match_timestamp","wallClock","ballAttributes",'ballPosition',"ballStatus","ballSpeed","ballTeam","ballInsideBox","ballInsideField")
    )


    (
        split_df
        .writeStream
        .format(file_format)
        .option('checkpointLocation',checkpoint)
        .option("path",file_location)
        .start()    
        .awaitTermination()
    )

    print("Streaming has finished")

if __name__ == '__main__':
    main()
