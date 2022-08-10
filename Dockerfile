############################
## Manage by : Azid
############################

FROM python:3.7.9-slim-buster

RUN mkdir -p /app && mkdir -p /secret1

COPY . /app/

WORKDIR /app/

RUN pip install --upgrade pip && pip install -r requirements.txt