from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from delta import *
from utils import ball_inside_box, read_config



def main():

    config = read_config()

    app_spark_name = config['spark_application']['batchApp']

    builder = (
        SparkSession.builder.appName(app_spark_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") 
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    df = spark.read.json('./data/g1059778_Data.jsonl')

    base_columns = ['period','frameIdx','gameClock','wallClock','live','lastTouch','match_date','ball_inside_box']

    temp_df = (
        df
        .withColumn('match_date', F.from_unixtime(F.col('wallClock')/1000, 'yyyy-MM-dd'))
        .withColumn('match_timestamp',F.from_unixtime(F.col('wallClock')/1000, 'yyyy-MM-dd HH:mm:ss:SSS'))
        .withColumn('ball_inside_box', ball_inside_box(F.col('ball')))
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
    )

    newData = spark.read.json('./data/g1060043_Data.jsonl')
    newData = (
        newData
        .withColumn('match_date', F.from_unixtime(F.col('wallClock')/1000, 'yyyy-MM-dd'))
        .withColumn('ball_inside_box', ball_inside_box(F.col('ball')))
    )

    newData = (
        newData
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
    )

    from pyspark.sql.window import Window

    windowTop = Window.partitionBy("away_home_team").orderBy(F.col("player_avg_speed").desc())

    columns = unified_df.columns

    home_columns = [col for col in columns if col.startswith('home')] + base_columns
    home_df = (
        unified_df
        .select(home_columns)
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

    away_columns = [col for col in columns if col.startswith('away')] + base_columns

    away_df = (
        unified_df
        .select(away_columns)
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

    windowDelta = Window.partitionBy("homePlayer_playerId").orderBy("period","gameClock")
    windowTimeDelta = Window.partitionBy("homePlayer_playerId").orderBy("period")

    (
        home_df
        .dropDuplicates()
        .withColumn('x_pos_lag',F.lag("home_player_3d_position_x",1).over(windowDelta))
        .withColumn('y_pos_lag',F.lag("home_player_3d_position_y",1).over(windowDelta))
        .withColumn("time_lag",F.lag("gameClock").over(windowTimeDelta))
        .fillna(0.0)
        .withColumn("delta_x",F.abs(F.col("home_player_3d_position_x") - F.col("x_pos_lag")))
        .withColumn("delta_y",F.abs(F.col("home_player_3d_position_y") - F.col("y_pos_lag")))
        .withColumn("delta_time",F.col("gameClock") - F.col("time_lag"))
        .withColumn("speed_x", F.col("delta_x")/F.col("delta_time"))
        .withColumn("speed_y", F.col("delta_y")/F.col("delta_time"))
        .groupBy("homePlayer_playerId")
        .agg(F.max("speed_x").alias("maximum_speed_x"))
    ).show(5,truncate=False)
