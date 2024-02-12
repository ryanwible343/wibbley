FROM python:3.11.2-slim as dev
RUN mkdir /app
COPY ./examples/durable_messagebus/. ./app
RUN mkdir wibbley_module
COPY . ./wibbley_module
WORKDIR /wibbley_module
RUN pip install -e .
RUN pip install sqlalchemy
RUN pip install psycopg2-binary
RUN pip install asyncpg
WORKDIR /app
