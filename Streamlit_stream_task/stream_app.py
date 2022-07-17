import streamlit as st
import subprocess
import sys
import time
from utils import plot_pitch, init_spark_session,read_config
from bs4 import BeautifulSoup
from datetime import datetime

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

    if read_stream:
        now = datetime.now()
        delta = 0
        init_ball_status = 'Alive'
        init_ball_position = False # ball is not in the penalty box at the beginning
        init_field_position = False
        fig,ax = plot_pitch((104.85,67.97))
        plot_spot = st.empty()
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
            if alive != init_ball_status:
                print(f'Timestamp: {ts}; Ball change status from {init_ball_status} to {alive}')
            
            if box_position != init_ball_position:
                if box_position:
                    text = f'Timestamp: {ts}; Action is coming! Ball is going to inside penalty box!; Position: {xy_position}'
                    col2.text_area(label ="",value=text, height =100)
                    ax.plot(float(xy_position[0]),float(xy_position[1]),'ro')
                else:
                    text = f'Timestamp: {ts}; There is no more danger! Ball is going outside the penalty box!; Position: {xy_position}'
                    col2.text_area(label ="",value=text, height =100)
                    ax.plot(float(xy_position[0]),float(xy_position[1]),'ro')
            
            if field_position != init_field_position:
                if field_position:
                    text = f'Timestamp: {ts}; Ball is back on the field!; Position: {xy_position}'
                    col2.text_area(label ="",value=text, height =100)
                    ax.plot(float(xy_position[0]),float(xy_position[1]),'ro')
                else:
                    text = f'Timestamp: {ts}; Ball is outside of the field!; Position: {xy_position}'
                    col2.text_area(label ="",value=text, height =100)
                    ax.plot(float(xy_position[0]),float(xy_position[1]),'ro')


            then = datetime.now()
            delta = (then - now).seconds
            init_ball_position = box_position
            init_field_position = field_position
            init_ball_status = alive
            with plot_spot:
                col1.write(fig)

if __name__ == '__main__':
    app()
