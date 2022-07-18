# Aston Villa technical challenge

Dear reviewer,

this readme is mainly for introduction to this repository. First of all, the whole solution is based on `spark` and `pyspark`, where both version needs to be the same. I am using the version `3.2.0`. The whole solution was developed on the local machine, therefore I face some issues (e.g low memory, not good configuration of the spark, etc.). If you are planning to install the spark on your machine (I am using `Ubuntu 20.04`), please follow [this manual](https://phoenixnap.com/kb/install-spark-on-ubuntu). The versions of the other packages can be found in `requirements.txt`

**Disclaimer:** I have not used `conda` environment (which was mistake).

The solution has 4 parts:
* `Batch_task` which represents the first batch task.
* `Stream_task` which represents the second stream task.
* `Streamlit_batch_task` which is fronted `streamlit` application/dashboard for the first task.
* `Streamlit_stream_task` which is frontend `streamlit` application/dashboard for the second task. The application is very similar to live score ;)

Each part has also own 'README.md` where is described functionality of each task.

The whole configuration for the each file/notebook is placed in `config.yaml`, where the user can defined lot of parameters such as data paths, format of the file, etc.

### What functionalities are missing ?


