FROM python:3.9.13-slim

COPY . /

RUN pip install -r /requirements.txt

ENTRYPOINT ["python", "/monitoring.py"]
