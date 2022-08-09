from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from delta import *
from utils import ball_inside_box, read_config
import os
import argparse
from pyspark.sql.window import Window
from bs4 import BeautifulSoup


# TODO: dat tu normalne natvrdo check, ci existuje uz zapisany frame, a ked nie, tak nech ho vytvori a 
# ked hej, tak nech pokracuje a nech zapise tu deltu tabulku..


parser = argparse.ArgumentParser()
# These arguments will be set appropriately by ReCodEx, even if you change them.
parser.add_argument("--data_path", default="<JSONL_FILE>", type=str, help="Path to data from particular match.")


def load_data(data_path):

    config = read_config()

    delta_feature_player_dir = config['batch']['delta_features_player_dir']
    delta_feature_ball_dir = config['batch']['delta_features_ball_dir']
    app_spark_name = config['spark_application']['spark_app_batch_name']

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
        .json(data_path)
    )

    match_date_df = (
        raw_match_data_df
        .withColumn('match_date', F.from_unixtime(F.col('wallClock')/1000, 'yyyy-MM-dd')) 
        .withColumn('match_timestamp',F.from_unixtime(F.col('wallClock')/1000, 'yyyy-MM-dd HH:mm:ss:S'))
    )

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
        .withColumn("ballInsideBox",ball_inside_box("ball.xyz",F.lit("inside_box")))
        .select(
            F.col("ball.xyz").alias("ballPosition"),
            F.col("ball.speed").alias("ballSpeed"),
            F.col("ballInsideBox"),
            *base_columns
        )
    )

    # Aggregation
    
    # Define the window functions
    windowTop = Window.partitionBy("away_home_team").orderBy(F.col("player_avg_speed").desc())

    windowDelta_home = Window.partitionBy("homePlayer_playerId").orderBy("period","gameClock")
    windowDelta_away = Window.partitionBy("awayPlayer_playerId").orderBy("period","gameClock")
    windowTimeDelta_home = Window.partitionBy("homePlayer_playerId").orderBy("period")
    windowTimeDelta_away = Window.partitionBy("awayPlayer_playerId").orderBy("period")

    # Take the relevant columns
    columns = unified_players_df.columns

    home_columns = [col for col in columns if col.startswith('home')] + base_columns
    away_columns = [col for col in columns if col.startswith('away')] + base_columns


    home_players_df = (
        unified_players_df
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
        unified_players_df
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
                "oldData.playerId = newData.playerId and oldData.match_date = oldData.match_date"
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

    # ball performance

    ball_perf_df = (
        unified_ball_df
        .withColumn('ball_seconds',F.when(F.col('ballInsideBox') == True, 0.04).otherwise(0))
    )

    ball_perf_df_final = (
        ball_perf_df
        .groupBy("ballInsideBox","match_date")
        .agg(
            (F.sum('ball_seconds')/60).alias("minutes_inside_box"),
                F.count("ballInsideBox").alias("n_times_inside_box")
        )
        .filter(F.col("ballInsideBox") == True)
    )

    if os.path.isdir(delta_feature_ball_dir):

        deltaTable = DeltaTable.forPath(spark, delta_feature_ball_dir)

        (
            deltaTable.alias('oldData')
            .merge(
                ball_perf_df_final.alias('newData'),
                "oldData.match_date = newData.match_date"
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
