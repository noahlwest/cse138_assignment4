FROM python:3.9.0-alpine

COPY assignment4.py /assignment4.py
COPY requirements.txt /requirements.txt

RUN chmod +rwx assignment4.py
RUN chmod +rwx requirements.txt

RUN pip install -r requirements.txt

#CMD sleep infinity #used for debugging
CMD python assignment4.py
