FROM python:3.8-slim-buster

WORKDIR /home/app/

COPY ./ /home/app/

COPY ./requirements.txt .

VOLUME /app/data 

ENV DEBUG=1

RUN pip install -r requirements.txt

ENV PYTHONPATH=${PYTHONPATH}:/home/app/

CMD ["bash", "-c", "gunicorn main:app -b 0.0.0.0:5000"]
