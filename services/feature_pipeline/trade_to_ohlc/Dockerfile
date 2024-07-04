FROM python:3.11.3-slim-buster

ENV PYTHONUNBUFFERED = 1

RUN pip install poetry 

WORKDIR /app 

COPY pyproject.toml poetry.lock /app/

RUN poetry install 

COPY . /app/

CMD ["poetry", "run", "python", "src/main.py"]

