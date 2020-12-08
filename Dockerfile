FROM python:3.6.9

RUN apt-get update
RUN apt-get install -y apt-utils
RUN apt-get update
RUN mkdir /app
COPY modules /app
COPY waker.py /app
WORKDIR /app
RUN apt-get install nano -y
RUN pip install --upgrade pip
RUN pip install -r modules
CMD ["bash"]