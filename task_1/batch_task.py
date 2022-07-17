from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from delta import *
from utils import ball_inside_box, read_config
import os
import argparse
from pyspark.sql.window import Window



# TODO: dat tu normalne natvrdo check, ci existuje uz zapisany frame, a ked nie, tak nech ho vytvori a 
# ked hej, tak nech pokracuje a nech zapise tu deltu tabulku..


parser = argparse.ArgumentParser()
# These arguments will be set appropriately by ReCodEx, even if you change them.
parser.add_argument("--data_path", default="g1059778_Data.jsonl", type=str, help="Path to data from particular match.")


def main(args):

    config = read_config()

    player_performance_path = config['batch']['delta_player_dir']

    app_spark_name = config['spark_application']['spark_app_batch_name']

    builder = (
        SparkSession.builder.appName(app_spark_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") 
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    df = spark.read.json(args.data_path)

    base_columns = ['period','frameIdx','gameClock','wallClock','live','lastTouch','match_date','ball_inside_box']

    temp_df = (
        df
        .withColumn('match_date', F.from_unixtime(F.col('wallClock')/1000, 'yyyy-MM-dd'))
        .withColumn('match_timestamp',F.from_unixtime(F.col('wallClock')/1000, 'yyyy-MM-dd HH:mm:ss:SSS'))
        .withColumn('ball_inside_box', ball_inside_box(F.col('ball.xyz')))
    )

    unified_df = (
        temp_df
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

    #here we should load our data 

    # if os.path.isdir(player_performance_path):

    #     deltaTable = DeltaTable.forPath(spark, player_performance_path)

    #     (
    #         deltaTable.alias('oldData')
    #         .merge(
    #             unified_df.alias('newData'),
    #             "oldData.match_date = newData.match_date"
    #         )
    #         .whenNotMatchedInsertAll()
    #         .execute()
    #     )
    # else:

    #     (
    #         unified_df
    #         .write
    #         .format('delta')
    #         .mode('overwrite')
    #         .partitionBy('match_date')
    #         .save(player_performance_path)
    #     )

    # unified_df = spark.read.format("delta").load(player_performance_path)

    # Aggregation
    
    windowTop = Window.partitionBy("away_home_team").orderBy(F.col("player_avg_speed").desc())

    columns = unified_df.columns

    home_columns = [col for col in columns if col.startswith('home')] + base_columns
    away_columns = [col for col in columns if col.startswith('away')] + base_columns

    home_df = (
        unified_df
        .select(home_columns)
        .dropDuplicates()
    )

    home_df_grouped = (
        home_df
        .groupBy('homePlayer_playerId')
        .agg(
            F.avg('homePlayer_speed').alias('player_avg_speed'),
            F.max('homePlayer_speed').alias('player_max_speed')
        )
        .withColumn('away_home_team',F.lit('home'))
        .withColumnRenamed('homePlayer_playerId','playerId')
    )

    away_df = (
        unified_df
        .select(away_columns)
        .dropDuplicates()
    )

    away_df_grouped = (
        away_df
        .groupBy('awayPlayer_playerId')
        .agg(
            F.avg('awayPlayer_speed').alias('player_avg_speed'),
            F.max('awayPlayer_speed').alias('player_max_speed')
        )
        .withColumn('away_home_team',F.lit('away'))
        .withColumnRenamed('awayPlayer_playerId','playerId')
    )

    players_performance_df = (
        home_df_grouped
        .union(away_df_grouped)
    )

    (
        players_performance_df
        .orderBy('playerId')
        .withColumn('rank',F.row_number().over(windowTop))
        .filter(F.col('rank') <= 3)
    ).show(10,truncate=False)

    windowDelta_home = Window.partitionBy("homePlayer_playerId").orderBy("period","gameClock")
    windowDelta_away = Window.partitionBy("awayPlayer_playerId").orderBy("period","gameClock")
    windowTimeDelta_home = Window.partitionBy("homePlayer_playerId").orderBy("period")
    windowTimeDelta_away = Window.partitionBy("awayPlayer_playerId").orderBy("period")

    (
        home_df
        .withColumn('x_pos_lag',F.lag("home_player_3d_position_x",1).over(windowDelta_home))
        .withColumn('y_pos_lag',F.lag("home_player_3d_position_y",1).over(windowDelta_home))
        .withColumn("time_lag",F.lag("gameClock").over(windowTimeDelta_home))
        .fillna(0.0)
        .withColumn("delta_x",F.abs(F.col("home_player_3d_position_x") - F.col("x_pos_lag")))
        .withColumn("delta_y",F.abs(F.col("home_player_3d_position_y") - F.col("y_pos_lag")))
        .withColumn("delta_time",F.col("gameClock") - F.col("time_lag"))
        .withColumn("speed_x", F.col("delta_x")/F.col("delta_time"))
        .withColumn("speed_y", F.col("delta_y")/F.col("delta_time"))
        .groupBy("homePlayer_playerId")
        .agg(F.max("speed_x").alias("maximum_speed_x"))
        .withColumnRenamed("homePlayer_playerId","playerId")
        .withColumn("away_home_team",F.lit("home"))
    ).show(5,truncate=False)

    (
        away_df
        .withColumn('x_pos_lag',F.lag("away_player_3d_position_x",1).over(windowDelta_away))
        .withColumn('y_pos_lag',F.lag("away_player_3d_position_y",1).over(windowDelta_away))
        .withColumn("time_lag",F.lag("gameClock").over(windowTimeDelta_away))
        .fillna(0.0)
        .withColumn("delta_x",F.abs(F.col("away_player_3d_position_x") - F.col("x_pos_lag")))
        .withColumn("delta_y",F.abs(F.col("away_player_3d_position_y") - F.col("y_pos_lag")))
        .withColumn("delta_time",F.col("gameClock") - F.col("time_lag"))
        .withColumn("speed_x", F.col("delta_x")/F.col("delta_time"))
        .withColumn("speed_y", F.col("delta_y")/F.col("delta_time"))
        .groupBy("awayPlayer_playerId")
        .agg(F.max("speed_x").alias("maximum_speed_x"))
        .withColumnRenamed("awayPlayer_playerId","playerId")
        .withColumn("away_home_team",F.lit("away"))
    ).show(5,truncate=False)

    # ball performance

    ball_perf = (
        temp_df
        .withColumn('ball_seconds',F.when(F.col('ball_inside_box') == True, 0.04).otherwise(0))
    )

    (
        ball_perf
        .groupBy("ball_inside_box")
        .agg(
            (F.sum('ball_seconds')/60).alias("minutes_inside_box"),
             F.count("ball_inside_box").alias("n_times_inside_box")
        )
        .filter(F.col("ball_inside_box") == True)
    ).show(5,truncate=False)

if __name__ == '__main__':
    args = parser.parse_args([] if "__file__" not in globals() else None)
    main(args)