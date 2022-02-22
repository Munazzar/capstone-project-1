FROM python:3

RUN apt-get update

RUN pip3 install requests

RUN pip3 install google-cloud

FROM google/cloud-sdk

RUN pip3 install google-cloud-pubsub

COPY . /app
WORKDIR /app



CMD python3 data_request_api.py