FROM python:3.9


RUN apt-get update -y

RUN apt -y install ./chrome/google-chrome-96.deb

COPY requirements.txt ./

RUN pip install -r requirements.txt

ENV APP_HOME /APP_HOME
WORKDIR ${APP_HOME}
COPY . ./

CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 main:app