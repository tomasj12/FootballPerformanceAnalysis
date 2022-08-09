# Player performance analysis

Dear,

this readme file is mainly for an introduction to this repository. First of all, the whole solution is based on `spark` and `pyspark`, where both version needs to be the same. The solution depends on version `3.2.0`. The whole solution was developed on the local machine, therefore I face some issues (e.g low memory, not good configuration of the spark, etc.). If you are planning to install the spark on your machine (I am using `Ubuntu 20.04`), please follow [this manual](https://phoenixnap.com/kb/install-spark-on-ubuntu). The versions of the other packages can be found in `requirements.txt`


The solution has four parts:
* `Batch_task` which represents the first batch task.
* `Stream_task` which represents the second stream task.
* `Streamlit_batch_task` which is fronted `streamlit` application/dashboard for the first task.
* `Streamlit_stream_task` which is frontend `streamlit` application/dashboard for the second task. The application is very similar to live score ;)

Each part has also own `README.md` where is described how each solution works.

The whole configuration for the each file/notebook is placed in `config.yaml`, where the user can define parameters such as data paths, format of the file, etc.

Please, replace my paths for yours.

### What functionalities are missing ?

There is a still lot of work on the solution. From my point of view, following points are main painpoints:

* There should be schema checking in `Batch` task, for every new data. We should always control if we have the desired data (garbage in, garbage out)
* Pyspark code can be more optimized.
* In `Stream` task, `delta` table should be used instead of ,,basic'' `parquet` file. With delta, we can have much more functionalities for the streaming task.
* `Feature Store` layer should be implemented upon the aggregated data from the `Batch` task.

