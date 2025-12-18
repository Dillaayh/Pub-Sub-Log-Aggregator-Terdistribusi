FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir requests==2.31.0 python-dateutil==2.8.2

COPY publisher.py .

CMD ["python", "publisher.py"]
