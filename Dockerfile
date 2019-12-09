FROM python:3.6.9

RUN mkdir /app
COPY modules /app
COPY waker.py /app
COPY service.sh /app
WORKDIR /app
RUN apt-get update
RUN apt-get install nano
RUN pip install --upgrade pip
RUN pip install -r modules
CMD ["bash"]