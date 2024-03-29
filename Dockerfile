FROM python:3.8.6-buster

COPY api /api
COPY data /data
COPY requirements.txt /requirements.txt

RUN pip install -r requirements.txt

CMD uvicorn api.fast:app --host 0.0.0.0 --port $PORT
