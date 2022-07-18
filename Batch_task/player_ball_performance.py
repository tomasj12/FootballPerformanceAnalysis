# Import the modules
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import pyspark.sql.types as T
from delta import *
from utils import plot_pitch, ball_inside_box, read_config
from bs4 import BeautifulSoup
import os
import argparse



parser = argparse.ArgumentParser()
parser.add_argument("--metadata_path", default="g1059778_Metadata.xml", type=str, help="Path to metadata from particular match.")

def main(args):

    config = read_config()

    app_name = config['spark_application']['spark_app_batch_name']

    builder = (
        SparkSession.builder.appName(app_name) 
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") 
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    meta_data_path = args.metadata_path
    delta_player_path = config['batch']['delta_player_dir']
    delta_ball_path = config['batch']['delta_ball_dir']
    delta_feature_player_dir = config['batch']['delta_features_player_dir']
    delta_feature_ball_dir = config['batch']['delta_features_ball_dir']


    with open(meta_data_path,'r') as f:
        metadata = f.read()

    match_metadata = BeautifulSoup(metadata,'xml')

    metadata_match_data = match_metadata.find('match').get('dtDate').split(' ')[0]
    field_x = float(match_metadata.find('match').get('fPitchXSizeMeters'))
    field_y = float(match_metadata.find('match').get('fPitchYSizeMeters'))
    metadata_field_dim = (field_x,field_y)

    print(f"Match date: {metadata_match_data}")
    print(f"Field dimension: {metadata_field_dim}")

    unified_data_players_df = (
        spark
        .read
        .format("delta")
        .load(delta_player_path)
    )

    unified_data_ball_df = (
        spark
        .read
        .format("delta")
        .load(delta_ball_path)
    )

    # Define the window functions
    windowTop = Window.partitionBy("away_home_team").orderBy(F.col("player_avg_speed").desc())

    windowDelta_home = Window.partitionBy("homePlayer_playerId").orderBy("period","gameClock")
    windowDelta_away = Window.partitionBy("awayPlayer_playerId").orderBy("period","gameClock")
    windowTimeDelta_home = Window.partitionBy("homePlayer_playerId").orderBy("period")
    windowTimeDelta_away = Window.partitionBy("awayPlayer_playerId").orderBy("period")

    # Take the relevant columns
    columns = unified_data_players_df.columns

    home_columns = [col for col in columns if col.startswith('home')] + base_columns
    away_columns = [col for col in columns if col.startswith('away')] + base_columns

    home_players_df = (
        unified_data_players_df
        .select(home_columns)
        .dropDuplicates()
    )

    home_grouped_df = (
        home_players_df
        .groupBy('homePlayer_playerId','match_date')
        .agg(
            F.avg('homePlayer_speed').alias('player_avg_speed'),
            F.max('homePlayer_speed').alias('player_max_speed')
        )
        .withColumn('away_home_team',F.lit('home'))
        .withColumnRenamed('homePlayer_playerId','playerId')
    )

    home_x_dir_df = (
        home_players_df
        .withColumn('x_pos_lag',F.lag("home_player_3d_position_x",1).over(windowDelta_home))
        .withColumn("time_lag",F.lag("gameClock").over(windowTimeDelta_home))
        .fillna(0.0)
        .withColumn("delta_x",F.abs(F.col("home_player_3d_position_x") - F.col("x_pos_lag")))
        .withColumn("delta_time",F.col("gameClock") - F.col("time_lag"))
        .withColumn("speed_x", F.col("delta_x")/F.col("delta_time"))
        .groupBy("homePlayer_playerId")
        .agg(F.max("speed_x").alias("maximum_speed_x"))
        .withColumnRenamed("homePlayer_playerId","playerId")
        .withColumn("away_home_team",F.lit("home"))
    )

    away_players_df = (
        unified_data_players_df
        .select(away_columns)
        .dropDuplicates()
    )

    away_grouped_df = (
        away_players_df
        .groupBy('awayPlayer_playerId','match_date')
        .agg(
            F.avg('awayPlayer_speed').alias('player_avg_speed'),
            F.max('awayPlayer_speed').alias('player_max_speed')
        )
        .withColumn('away_home_team',F.lit('away'))
        .withColumnRenamed('awayPlayer_playerId','playerId')
    )

    away_x_dir_df = (
        away_players_df
        .withColumn('x_pos_lag',F.lag("away_player_3d_position_x",1).over(windowDelta_away))
        .withColumn("time_lag",F.lag("gameClock").over(windowTimeDelta_away))
        .fillna(0.0)
        .withColumn("delta_x",F.abs(F.col("away_player_3d_position_x") - F.col("x_pos_lag")))
        .withColumn("delta_time",F.col("gameClock") - F.col("time_lag"))
        .withColumn("speed_x", F.col("delta_x")/F.col("delta_time"))
        .groupBy("awayPlayer_playerId")
        .agg(F.max("speed_x").alias("maximum_speed_x"))
        .withColumnRenamed("awayPlayer_playerId","playerId")
        .withColumn("away_home_team",F.lit("away"))
    )

    players_performance_df = (
        home_grouped_df
        .union(away_grouped_df)
    )

    speed_x_dir_df = (
        home_x_dir_df
        .union(away_x_dir_df)
    )


    player_perf_final_df = (
        players_performance_df
        .join(speed_x_dir_df.select("playerId","maximum_speed_x"), on = ['playerId'], how = 'left')
    )

    if os.path.isdir(delta_feature_player_dir):

        deltaTable = DeltaTable.forPath(spark, delta_feature_player_dir)

        (
            deltaTable.alias('oldData')
            .merge(
                player_perf_final_df.alias('newData'),
                "oldData.playerId = newData.playerId"
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:

        (
            player_perf_final_df
            .write
            .format('delta')
            .mode('overwrite')
            .partitionBy('match_date')
            .save(delta_feature_player_dir)
        )
    
    ball_perf_df = (
        unified_data_ball_df
        .withColumn('ball_seconds',F.when(F.col('ball_inside_box') == True, 0.04).otherwise(0))
    )

    ball_perf_df_final = (
        ball_perf_df
        .groupBy("ball_inside_box")
        .agg(
            (F.sum('ball_seconds')/60).alias("minutes_inside_box"),
                F.count("ball_inside_box").alias("n_times_inside_box")
        )
        .filter(F.col("ball_inside_box") == True)
    )

    if os.path.isdir(delta_feature_ball_dir):

        deltaTable = DeltaTable.forPath(spark, delta_feature_ball_dir)

        (
            deltaTable.alias('oldData')
            .merge(
                ball_perf_df_final.alias('newData'),
                "oldData.playerId = newData.playerId"
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:

        (
            ball_perf_df_final
            .write
            .format('delta')
            .mode('overwrite')
            .partitionBy('match_date')
            .save(delta_feature_ball_dir)
        )



    

