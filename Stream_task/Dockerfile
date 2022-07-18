# Get the latest base image for python
FROM python:latest
ARG DATA_PATH
# Put files at the image '/server/' folder.
ADD ipc_server.py /server/
COPY $DATA_PATH /data/
# '/server/' is base directory
WORKDIR /server/
# Expose port 9898 in the container
EXPOSE 9898
# execute the command
CMD [ "python3", "/server/ipc_server.py" ]