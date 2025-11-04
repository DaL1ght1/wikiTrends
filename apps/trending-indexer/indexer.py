import os, time, ujson, hashlib
from kafka import KafkaConsumer
from opensearchpy import OpenSearch, helpers

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP","kafka:9092")
INPUT_TOPICS = os.getenv("INPUT_TOPICS","trending.windows.1m,trending.windows.5m").split(",")
OS_URL = os.getenv("OPENSEARCH_URL","http://opensearch:9200")
INDEX_1M = os.getenv("INDEX_1M","trending-1m")
INDEX_5M = os.getenv("INDEX_5M","trending-5m")

def doc_id(d):
    # stable id to avoid duplicates
    base = f"{d.get('ts_bucket')}|{d.get('wiki')}|{d.get('title')}|{d.get('window_size')}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()

def pick_index(d):
    return INDEX_1M if d.get("window_size")=="1m" else INDEX_5M

def main():
    consumer = KafkaConsumer(
        *INPUT_TOPICS,
        bootstrap_servers=BOOTSTRAP,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda v: ujson.loads(v.decode("utf-8"))
    )
    os_client = OpenSearch(OS_URL, timeout=30, max_retries=3, retry_on_timeout=True)
    buf, BATCH = [], 500
    while True:
        for msg in consumer:
            d = msg.value
            d["url"] = f"https://{d.get('lang','en')}.wikipedia.org/wiki/{str(d.get('title','')).replace(' ','_')}"
            _id = doc_id(d)
            idx = pick_index(d)
            buf.append({"_op_type":"index","_index":idx,"_id":_id,"_source":d})
            if len(buf) >= BATCH:
                helpers.bulk(os_client, buf, raise_on_error=False)
                buf.clear()

if __name__ == "__main__":
    main()
