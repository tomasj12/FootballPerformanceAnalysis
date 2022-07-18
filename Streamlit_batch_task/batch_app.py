import streamlit as st
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from delta import *
from utils import ball_inside_box, read_config
import os
import argparse
from pyspark.sql.window import Window
from batch_task_data import load_data
from bs4 import BeautifulSoup
from pathlib import Path


def app():
    
    st.title('Player Performance')

    config = read_config()

    player_performance_path = config['batch']['delta_features_player_dir']
    ball_performance_path = config['batch']['delta_features_ball_dir']

    app_spark_name = config['spark_application']['spark_app_batch_name']

    builder = (
        SparkSession.builder.appName(app_spark_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") 
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()


    data_path = st.text_input("Path to raw match data","")
    metadata_path = st.text_input("Path to metadata","")

    @st.cache
    def write_delta(data_path):
        load_data(data_path)
    
    @st.cache
    def delta_to_pd(delta_path,filters=None):
        df = (
            spark
            .read
            .format("delta")
            .load(delta_path)
        )
        if filters:
            if len(filters) == 1 and filters[0] == 'The fastest players':
                    windowTop = Window.partitionBy("away_home_team","match_date").orderBy(F.col("player_avg_speed").desc())
                    df = (
                            df
                            .orderBy('playerId')
                            .withColumn('rank',F.row_number().over(windowTop))
                            .filter(F.col('rank') <= 5)
                            .orderBy('away_home_team','match_date','rank')
                        )
            if len(filters) == 1 and filters[0] == 'The slowest players': 
                    windowWorst = Window.partitionBy("away_home_team","match_date").orderBy(F.col("player_avg_speed").asc())
                    df = (
                            df
                            .orderBy('playerId')
                            .withColumn('rank',F.row_number().over(windowWorst))
                            .filter(F.col('rank') <= 5)
                            .orderBy('away_home_team','match_date','rank')
                        )
            else:
                pass
        
        df_pd = df.toPandas()
        return df_pd

    if data_path:
        if metadata_path:
            with open(metadata_path,'r') as f:
                metadata = f.read()
            bs = BeautifulSoup(metadata, 'xml')
            match_date = bs.find('match').get('dtDate').split(' ')[0]
            if not os.path.isdir(Path(player_performance_path)/ f"match_date={match_date}"):
                data_load_state = st.text('Loading data...')
                write_delta(data_path=data_path)
                data_load_state.text("Done! (using st.cache)")

                data_load_state = st.text('Transfering delta table to pandas data frame')
                df_pd_players = delta_to_pd(delta_path=player_performance_path)
                df_pd_ball = delta_to_pd(delta_path=ball_performance_path)
                data_load_state.text("Done! (using st.cache)")

            else:
                data_load_state = st.text('')
                df_pd_players = delta_to_pd(delta_path=player_performance_path)
                df_pd_ball = delta_to_pd(delta_path=ball_performance_path)
                data_load_state.text("Done! (using st.cache)")

    col1,col2,col3 = st.columns(3)
    
    if st.checkbox('Show raw data'):
        df_pd_players_home = df_pd_players[df_pd_players['away_home_team'] == 'home']
        df_pd_players_away = df_pd_players[df_pd_players['away_home_team'] == 'away']
        col1.subheader('Sample from home team players')
        col1.write(df_pd_players_home.sample(10))
        col2.subheader('Sample from home team players')
        col2.write(df_pd_players_away)
        col3.subheader("Ball performance")
        col3.write(df_pd_ball)

    options = st.multiselect(
            'Player performances',
            ['The fastest players', 'The slowest players'],
            )
    
    col4,col5 = st.columns(2)

    if options:
        stat_data = delta_to_pd(delta_path=player_performance_path, filters=options)
        stat_data_home = stat_data[stat_data['away_home_team'] == 'home']
        stat_data_away = stat_data[stat_data['away_home_team'] == 'away']
        col4.subheader('Home team' + options[0])
        col4.write(stat_data_home)
        col5.subheader('Away team' + options[0])
        col5.write(stat_data_away)

    

if __name__ == '__main__':
    app()