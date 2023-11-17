FROM python:3.9.1

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2 python-decouple

WORKDIR /app
COPY upload_data.py upload_data.py
COPY .env .env

ENTRYPOINT [ "python","upload_data.py" ]