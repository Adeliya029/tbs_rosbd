FROM prefecthq/prefect:3-latest

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["prefect", "worker", "start", "--pool", "scraping-pool"]
