FROM python:3.10-slim-buster as builder

WORKDIR  /dctask
COPY . /dctask

RUN pip install requests \
    pip install pyyaml

ENTRYPOINT ["python","/dctask/dctask.py"]