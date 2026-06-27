FROM python:3.12-slim

WORKDIR /library_rest_api

COPY requirements.txt .

RUN pip install --upgrade pip
RUN pip install -r requirements.txt

COPY app_library .