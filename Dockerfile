FROM python:3.6.9

RUN mkdir /app
COPY modules /app
COPY waker.py /app
WORKDIR /app
RUN pip install -r modules
CMD ["bash"]