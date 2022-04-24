FROM python:3.8.6-buster

COPY app /app
COPY data /data
COPY requirements.txt /requirements.txt

RUN pip install -r requirements.txt

CMD uvicorn api.fast:app --host 0.0.0.0 --port 8000
