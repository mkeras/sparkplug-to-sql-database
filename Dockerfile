FROM python:3.10-buster

WORKDIR /recorder

RUN git clone https://github.com/mkeras/sparkplug-b-dataclasses


COPY ./src/requirements.txt /recorder/requirements.txt

RUN pip install -r /recorder/requirements.txt

COPY ./src ./

RUN mv ./sparkplug-b-dataclasses ./app/sparkplug_b

CMD python app

