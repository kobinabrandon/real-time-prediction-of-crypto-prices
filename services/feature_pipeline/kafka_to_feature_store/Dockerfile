FROM python:3.11.9-bullseye

ENV PYTHONUNBUFFERED = 1 

RUN pip install poetry

WORKDIR /app/

COPY pyproject.toml poetry.lock /app/

RUN poetry install 

COPY . /app 

CMD ["poetry", "run", "python", "src/main.py"]