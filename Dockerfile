FROM astrocrpublic.azurecr.io/runtime:3.1-13

# Copy ETL modules and config into the image
COPY etls/ /usr/local/airflow/etls/
COPY include/ /usr/local/airflow/include/

# Install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
