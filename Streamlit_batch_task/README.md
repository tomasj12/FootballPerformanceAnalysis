# Streamlit application: Batch task

**Goal** of this application is to display the aggregations upon the match data.

Because there is always consumer at the other side of the data (coach, analyst, data scientist), it is not about the ingest and data loading. We need to give to data a visual side, somehow. 

In this folder, user can found streamlit application `batch_app.py`, which functionality is to show the following:

* The raw data
* The fastest/slowest players
* The performance of the ball

The simple animation can be found in `streamlit-batch_app-2022-07-18-22-07-74.gif`.

For running the application, `streamlit` module is needed. 

```sh
pip install streamlit
```

If the user want to run the application:

```sh
streamlit run batch_app.py
```

Application then appear on some localhost link, which can be found in the console (or automatically redirect to application). 

For the correctly running the application, the user need to follow the following points:

* In the first text input, user needs to copy paste the path to raw data
* In the second text input, user needs to copy paste the path to metadata
* Then push the `Enter` button
* If the message `Done` is received, user can check the `show the raw data` check box to show the sample of the aggregated data.
* At the end, user can display the fastest/slowest players in the multiselect in the last line. Unfortunatey, it is possible to have **only one option at once**.
