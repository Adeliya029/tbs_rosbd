FROM apache/spark:3.5.0

USER root

RUN pip3 install --upgrade pip

RUN pip3 install \
    pandas \
    numpy \
    scikit-learn \
    joblib \
    boto3 \
    pyarrow

USER spark