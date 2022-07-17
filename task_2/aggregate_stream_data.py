from pyspark.sql import SparkSession
from utils import read_config, init_spark_session,plot_pitch
import time
from datetime import datetime
import matplotlib.pyplot as plt

def main():

    config = read_config()

    spark_app_name = config['spark_application']['spark_app_stream_name']

    file_format = config['streaming']['format']
    file_dir = config['streaming']['file_dir']

    agg_time = config['streaming_aggregation']['aggregating_time_s']
    wait_time = config['streaming_aggregation']['wait_for_agg_s']

    spark = init_spark_session(spark_app_name)
    plt.ion()

    now = datetime.now()
    delta = 0
    init_ball_status = 'Alive'
    init_ball_position = False # ball is not in the penalty box at the beginning
    init_field_position = False
    fig,ax = plot_pitch((104.85,67.97))

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
                print(f'Timestamp: {ts}; Action is coming! Ball is going to inside penalty box!; Position: {xy_position}')
                ax.plot(float(xy_position[0]),float(xy_position[1]),'ro')
            else:
                print(f'Timestamp: {ts}; There is no more danger! Ball is going outside the penalty box!; Position: {xy_position}')
                ax.plot(float(xy_position[0]),float(xy_position[1]),'ro')
        
        if field_position != init_field_position:
            if field_position:
                print(f'Timestamp: {ts}; Ball is back on the field!; Position: {xy_position}')
                ax.plot(float(xy_position[0]),float(xy_position[1]),'ro')
            else:
                print(f'Timestamp: {ts}; Ball is outside of the field!; Position: {xy_position}')
                ax.plot(float(xy_position[0]),float(xy_position[1]),'ro')


        then = datetime.now()
        delta = (then - now).seconds
        init_ball_position = box_position
        init_field_position = field_position
        init_ball_status = alive

        # drawing updated values
        fig.canvas.draw()
 
        # This will run the GUI event
        # loop until all UI events
        # currently waiting have been processed
        fig.canvas.flush_events()

        print(delta)


    print("Finisihed") 

if __name__ == '__main__':
    main()
        