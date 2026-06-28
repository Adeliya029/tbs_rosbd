FROM apache/spark:3.5.0

USER root
RUN apt-get update -qq && apt-get install -y -qq netcat-openbsd curl && rm -rf /var/lib/apt/lists/*

USER spark
