import streamlit as st
import subprocess
import sys
import time
from utils import plot_pitch, init_spark_session,read_config,check_throw_corner
from bs4 import BeautifulSoup
from datetime import datetime
import os

def app():

    config = read_config()

    spark_app_name = config['spark_application']['spark_app_stream_name']

    file_format = config['streaming']['format']
    file_dir = config['streaming']['file_dir']

    agg_time = config['streaming_aggregation']['aggregating_time_s']
    wait_time = config['streaming_aggregation']['wait_for_agg_s']

    spark = init_spark_session(spark_app_name)
    
    st.title('Live football events')

    read_stream = st.button("Read Stream")

    with open("/home/tomas/Personal_projects/Aston_Villa/data/g1059783_Metadata.xml",'r') as f:
        metadata = f.read()

    bs = BeautifulSoup(metadata,'xml')
    x_size = float(bs.find('match').get('fPitchXSizeMeters'))
    y_size = float(bs.find('match').get('fPitchYSizeMeters'))
    pitch_dim = (x_size,y_size)

    fig,ax = plot_pitch(pitch_dim)

    col1, col2 = st.columns(2)

    live_update = ''

    if read_stream:
        now = datetime.now()
        delta = 0
        init_ball_status = 'Alive'
        init_ball_position = False # ball is not in the penalty box at the beginning
        init_field_position = True
        field_dimen = (104.85,67.97)
        fig,ax = plot_pitch(field_dimen)
        text_area_placeholder = col2.empty()
        plot_area = col1.empty()
        xy_init = (0,0)
        

        if os.path.isdir(file_dir):
            
            while delta < agg_time:

                ax.plot(float(xy_init[0]), float(xy_init[1]), 'ro')
                plot_area.write(fig, key = delta)
                
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
                    live_update = live_update + '\n' + text
                
                if box_position != init_ball_position:
                    if box_position:
                        text = f'Timestamp: {ts}; Action is coming! Ball is going to inside penalty box!; Position: {xy_position}'
                        print(text)
                        live_update = live_update + '\n' + text
                        ax.plot(float(xy_position[0]),float(xy_position[1]),'ro')
                    else:
                        text = f'Timestamp: {ts}; There is no more danger! Ball is going outside the penalty box!; Position: {xy_position}'
                        print(text)
                        live_update = live_update + '\n' + text
                        ax.plot(float(xy_position[0]),float(xy_position[1]),'ro')
                
                if field_position != init_field_position:
                    if field_position:
                        text = f'Timestamp: {ts}; Ball is back on the field!; Position: {xy_position}'
                        print(text)
                        live_update = live_update + '\n' + text
                        ax.plot(float(xy_position[0]),float(xy_position[1]),'ro')
                    else:

                        pos,adjust_xy_pos = check_throw_corner(field_dimen=field_dimen, ball_pos=xy_position)
                        team = 'home team' if team_last == 'A' else 'away team'
                        throw_corner = f'{pos} for {team}'
                        text = f'Timestamp: {ts}; Ball is outside of the field!; {throw_corner}; Position: {xy_position}'
                        print(text)
                        live_update = live_update + '\n' + text
                        ax.plot(float(adjust_xy_pos[0]),float(adjust_xy_pos[1]),'ro')
                        

                if len(live_update) > 0:
                    text_area_placeholder.text_area(label ="",value=live_update, height =400, key = delta)

                ax.plot(float(xy_init[0]), float(xy_init[1]), color = 'mediumseagreen', marker='o', mec = 'mediumseagreen')
                plot_area.write(fig, key = delta)


                then = datetime.now()
                delta = (then - now).seconds
                init_ball_position = box_position
                init_field_position = field_position
                init_ball_status = alive
                xy_init = xy_position

if __name__ == '__main__':
    app()
