FROM python:3.6.9

RUN apt-get update -y
RUN apt-get install -y apt-utils
RUN mkdir /pyx
COPY requirements.txt /pyx
WORKDIR /pyx
RUN apt-get install nano -y
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
CMD ["bash"]