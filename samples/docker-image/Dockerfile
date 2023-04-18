FROM python:3.10-slim-bullseye as base

WORKDIR /app
ENV PYTHONPATH="/app"
COPY ./main.py .

RUN apt-get update
RUN pip install heizer

ENTRYPOINT [ "/bin/bash", "-c", "python main.py" ]
