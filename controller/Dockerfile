FROM python:3.10-slim

WORKDIR /app

COPY app.py /app/

RUN pip install fastapi uvicorn pydantic pymongo aiormq

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "5000"]
