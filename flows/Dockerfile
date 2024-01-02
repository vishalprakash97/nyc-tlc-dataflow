FROM prefecthq/prefect:2.14.6-python3.11

COPY docker-requirements.txt  .

RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org --no-cache-dir

COPY gcp/parameterized_web_to_gcs.py /opt/prefect/flows/parameterized_web_to_gcs.py

