FROM apache/airflow:2.4.3-python3.8
USER root
RUN apt-get update
RUN apt-get install -y gcc postgresql libpq-dev git nano
USER airflow
RUN python3 -m pip install --upgrade pip
COPY extended_image/requirements.txt /opt/app/requirements.txt
WORKDIR /opt/app
RUN python3 -m pip install -r requirements.txt
WORKDIR /opt/airflow/dags