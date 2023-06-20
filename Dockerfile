FROM python:3

RUN mkdir /app
WORKDIR /app
COPY pyproject.toml poetry.lock /app/
ENV PYTHONPATH=${PYTHONPATH}:${PWD}
ENV PIP_ROOT_USER_ACTION=ignore
RUN pip install poetry && poetry config virtualenvs.create false && poetry install --no-root --no-directory
COPY mbstats /app/mbstats
COPY README.md /app/
RUN poetry install --only main


