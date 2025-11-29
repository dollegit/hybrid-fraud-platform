FROM apache/spark:3.4.1-python3

USER root
WORKDIR /opt/spark/work-dir

# Add your test script from the src directory
COPY ./src/simple_print.py .

USER spark