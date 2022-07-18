# Streamlit application: Streaming task

**Goal** of this application is somehow to vizualize the live/stream data. 

Because there is always consumer at the other side of the data (coach, analyst, data scientist), it is not about the ingest and data loading. We need to give to data a visual side, somehow. 

In this folder, user can found streamlit application `stream_app.py`, which functionality is to show the following:

* How the ball is moved on the pitch
* If the ball is outside of the field - if corner or throw in (home/away team)
* If the ball is dead/alive
* If the ball is inside/outside box.

The simple animation can be found in `streamlit-stream_app-2022-07-18-20-07-44.gif`.

For running the application, `streamlit` module is needed. 

```sh
pip install streamlit
```

Again, as in the previous stream task, the order of exection is important:

1. First run  `docker run -p 9898:9898 streaming_process`
2. Run `read_stream_data.py` in `Batch_stream` folder
3. Run `streamlit run stream_app.py`

Application then appear on some localhost link, which can be found in the console (or automatically redirect to application). 

If the user is in the application, for the starting reading the stream, one need to push a `Read stream` button.