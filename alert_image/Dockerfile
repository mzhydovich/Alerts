FROM python:3.8

WORKDIR /alert_image

COPY . /alert_image

RUN pip install -r requirements.txt

CMD [ "python", "./alert.py" ]
