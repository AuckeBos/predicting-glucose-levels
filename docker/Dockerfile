FROM python:3.8
LABEL maintainer="Aucke Bos"

RUN pip install poetry
COPY pyproject.toml poetry.lock /app/
COPY predicting_glucose_levels /app/predicting_glucose_levels
WORKDIR /app

RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi


ENTRYPOINT ["tail", "-f", "/dev/null"]