from pyspark.sql import SparkSession
from utils import read_config, init_spark_session,plot_pitch,check_throw_corner
import time
from datetime import datetime
import matplotlib.pyplot as plt
import os
import argparse
from bs4 import BeautifulSoup

parser = argparse.ArgumentParser()
parser.add_argument("--metadata_path", default="g1059778_Metadata.xml", type=str, help="Path to metadata from particular match.")


def main(args):

    config = read_config()

    spark_app_name = config['spark_application']['spark_app_stream_name']

    file_format = config['streaming']['format']
    file_dir = config['streaming']['file_dir']

    agg_time = config['streaming_aggregation']['aggregating_time_s']
    wait_time = config['streaming_aggregation']['wait_for_agg_s']

    spark = init_spark_session(spark_app_name)
    plt.ion()

    with open(args.meta_data_path,'r') as f:
        metadata = f.read()

    bs = BeautifulSoup(metadata,'xml')
    x_size = float(bs.find('match').get('fPitchXSizeMeters'))
    y_size = float(bs.find('match').get('fPitchYSizeMeters'))
    pitch_dim = (x_size,y_size)


    now = datetime.now()
    delta = 0
    init_ball_status = 'Alive'
    init_ball_position = False # ball is not in the penalty box at the beginning
    init_field_position = True
    field_dimen = (104.85,67.97)
    fig,ax = plot_pitch(field_dimen)
    
    xy_init = (0,0)

    if os.path.isdir(file_dir):
            
        while delta < agg_time:

            
            time.sleep(wait_time)
            df = (
                spark.read.format(file_format).load(file_dir)
            )
            df_pd = df.toPandas()
            df_pd = df_pd.sort_values("wallClock")
            df_pd = df_pd.tail(1)
            alive = df_pd['ballStatus'].values[0]
            box_position = df_pd['ballInsideBox'].values[0]
            xy_position = df_pd['ballPosition'].values[0]
            ts= df_pd['match_timestamp'].values[0]
            field_position = df_pd['ballInsideField'].values[0]
            team_last = df_pd['ballTeam'].values[0]

            if alive != init_ball_status:
                text = f'Timestamp: {ts}; Ball change status from {init_ball_status} to {alive}'
                print(text)
            
            if box_position != init_ball_position:
                if box_position:
                    text = f'Timestamp: {ts}; Action is coming! Ball is going to inside penalty box!; Position: {xy_position}'
                    print(text)
                else:
                    text = f'Timestamp: {ts}; There is no more danger! Ball is going outside the penalty box!; Position: {xy_position}'
                    print(text)
            
            if field_position != init_field_position:
                if field_position:
                    text = f'Timestamp: {ts}; Ball is back on the field!; Position: {xy_position}'
                    print(text)
                else:

                    pos,adjust_xy_pos = check_throw_corner(field_dimen=field_dimen, ball_pos=xy_position)
                    team = 'home team' if team_last == 'A' else 'away team'
                    throw_corner = f'{pos} for {team}'
                    text = f'Timestamp: {ts}; Ball is outside of the field!; {throw_corner}; Position: {xy_position}'
                    print(text)
                    


            then = datetime.now()
            delta = (then - now).seconds
            init_ball_position = box_position
            init_field_position = field_position
            init_ball_status = alive
            xy_init = xy_position


    print("Finisihed") 

if __name__ == '__main__':
    
    args = parser.parse_args([] if "__file__" not in globals() else None)
    main(args)
        