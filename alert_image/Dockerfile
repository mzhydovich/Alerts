FROM python:3.8

COPY ./src ./
COPY ./config.json ./
COPY ./requirements.txt ./

RUN pip install -r requirements.txt

CMD [ "python", "./alert.py" ]
