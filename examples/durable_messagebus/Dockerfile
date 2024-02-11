FROM python:3.11.2-slim as dev
RUN mkdir /app
COPY ./examples/durable_messagebus/. ./app
RUN mkdir wibbley_module
COPY . ./wibbley_module
WORKDIR /wibbley_module
RUN pip install -e .
WORKDIR /app
