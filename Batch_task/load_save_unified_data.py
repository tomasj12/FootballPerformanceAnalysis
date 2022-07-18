from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from delta import *
from utils import ball_inside_box, read_config
import os
import argparse
from pyspark.sql.window import Window
from bs4 import BeautifulSoup

parser = argparse.ArgumentParser()
parser.add_argument("--data_path", default="g1059778_Data.jsonl", type=str, help="Path to data from particular match.")
parser.add_argument("--metadata_path", default="g1059778_Metadata.xml", type=str, help="Path to metadata from particular match.")



def main(args):

    config = read_config()

    delta_player_path = config['batch']['delta_player_dir']
    delta_ball_path = config['batch']['delta_ball_dir']
    app_spark_name = config['spark_application']['spark_app_batch_name']

    builder = (
        SparkSession.builder.appName(app_spark_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") 
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    raw_match_data_df = (
        spark
        .read
        .json(args.data_path)
    )


    with open(args.meta_data_path,'r') as f:
        metadata = f.read()

    match_metadata = BeautifulSoup(metadata,'xml')

    metadata_match_data = match_metadata.find('match').get('dtDate').split(' ')[0]
    field_x = float(match_metadata.find('match').get('fPitchXSizeMeters'))
    field_y = float(match_metadata.find('match').get('fPitchYSizeMeters'))
    metadata_field_dim = (field_x,field_y)

    print(f"Match date: {metadata_match_data}")
    print(f"Field dimension: {metadata_field_dim}")


    match_date_df = (
        raw_match_data_df
        .withColumn('match_date', F.from_unixtime(F.col('wallClock')/1000, 'yyyy-MM-dd')) 
        .withColumn('match_timestamp',F.from_unixtime(F.col('wallClock')/1000, 'yyyy-MM-dd HH:mm:ss:S'))
    )

    (
        match_date_df
        .select('wallClock','match_date','match_timestamp')
    ).show(5,truncate=False)

    base_columns = ['period','frameIdx','gameClock','wallClock','live','lastTouch','match_date']

    unified_players_df = (
        match_date_df
        .withColumn('home_players_exploded',F.explode('homePlayers')) 
        .withColumn('away_players_exploded',F.explode('awayPlayers'))
        .select(
            F.col('home_players_exploded.playerId').alias('homePlayer_playerId'),
            F.col('home_players_exploded.speed').alias('homePlayer_speed'),
            F.col('home_players_exploded.xyz').alias('homePlayer_3d_position'),
            F.col('away_players_exploded.playerId').alias('awayPlayer_playerId'),
            F.col('away_players_exploded.speed').alias('awayPlayer_speed'),
            F.col('away_players_exploded.xyz').alias('awayPlayer_3d_position'),
            *base_columns
        )
        .withColumn("home_player_3d_position_x", F.col('homePlayer_3d_position').getItem(0))
        .withColumn("home_player_3d_position_y", F.col('homePlayer_3d_position').getItem(1))
        .withColumn("home_player_3d_position_z", F.col('homePlayer_3d_position').getItem(2))
        .withColumn("away_player_3d_position_x", F.col('awayPlayer_3d_position').getItem(0))
        .withColumn("away_player_3d_position_y", F.col('awayPlayer_3d_position').getItem(1))
        .withColumn("away_player_3d_position_z", F.col('awayPlayer_3d_position').getItem(2))
    )

    unified_ball_df = (
        match_date_df
        .select(
            F.col("ball.xyz").alias("ballPosition"),
            F.col("ball.speed").alias("ballSpeed"),
            *base_columns
        )
    )

    if os.path.isdir(delta_player_path):

        deltaTable = DeltaTable.forPath(spark, delta_player_path)

        (
            deltaTable.alias('oldData')
            .merge(
                unified_players_df.alias('newData'),
                "oldData.match_date = newData.match_date"
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:

        (
            unified_players_df
            .write
            .format('delta')
            .mode('overwrite')
            .partitionBy('match_date')
            .save(delta_player_path)
        )
    
    if os.path.isdir(delta_ball_path):

        deltaTable = DeltaTable.forPath(spark, delta_ball_path)

        (
            deltaTable.alias('oldData')
            .merge(
                unified_ball_df.alias('newData'),
                "oldData.match_date = newData.match_date"
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:

        (
            unified_ball_df
            .write
            .format('delta')
            .mode('overwrite')
            .partitionBy('match_date')
            .save(delta_player_path)
        )

if __name__ == '__main__':

    args = parser.parse_args([] if "__file__" not in globals() else None)
    main(args)






