FROM apache/airflow:2.8.0
COPY extended_dockerfile_image_files/requirements.txt /requirements.txt
COPY permissions-417120-8c4dc3a5c773.json /permissions-417120-8c4dc3a5c773.json
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt

