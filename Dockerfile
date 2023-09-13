FROM python:3.11.1

WORKDIR /home/ntools

RUN apt-get update
RUN pip install --upgrade pip
RUN pip install pipenv

ADD Pipfile Pipfile
ADD Pipfile.lock Pipfile.lock
RUN pipenv install

