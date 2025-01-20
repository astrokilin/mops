FROM python:3.10-slim

WORKDIR /app

COPY app.py /app/

RUN pip install pymongo aiormq

CMD ["python", "app.py"]
