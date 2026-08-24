FROM public.ecr.aws/lambda/python:3.13

COPY ingestion/onchain/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Code only — no reference data baked into images (repo convention).
COPY ingestion/__init__.py /var/task/ingestion/__init__.py
COPY ingestion/common/__init__.py ingestion/common/partitions.py /var/task/ingestion/common/
COPY ingestion/onchain/__init__.py ingestion/onchain/extract.py ingestion/onchain/handler.py /var/task/ingestion/onchain/

CMD ["ingestion.onchain.handler.handler"]
