import os, base64, ujson, requests
from kafka import KafkaConsumer
from datetime import datetime

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP","kafka:9092")
INPUT_TOPICS = os.getenv("INPUT_TOPICS","trending.windows.1m,trending.windows.5m").split(",")
HBASE_REST = os.getenv("HBASE_REST_URL","http://hbase:8085")
TABLE = os.getenv("HBASE_TABLE","rc_clean_minute")

def b64(s):
    if isinstance(s, str):
        s = s.encode("utf-8")
    return base64.b64encode(s).decode("ascii")

def rowkey(d):
    # row key: wiki|title|yyyymmddHHmm (minute bucket)
    ts = d.get("ts_bucket")
    # ts is ISO from Spark; keep as minute resolution
    minute = ts[:16].replace("-","").replace(":","").replace("T","")
    return f"{d.get('wiki')}|{d.get('title')}|{minute}"

def to_cells(d):
    m = {
      "edit_count": d.get("edit_count",0),
      "unique_editors": d.get("unique_editors",0),
      "sum_abs_delta": d.get("sum_abs_delta",0),
      "revert_count": d.get("revert_count",0),
      "minor_share": d.get("minor_share",0.0),
      "bot_share": d.get("bot_share",0.0),
      "score": d.get("score",0.0),
      "window_size": d.get("window_size","1m")
    }
    cells = []
    for k,v in m.items():
        cells.append({"column": b64(f"m:{k}"), "$": b64(str(v))})
    return cells

def main():
    consumer = KafkaConsumer(
        *INPUT_TOPICS,
        bootstrap_servers=BOOTSTRAP,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda v: ujson.loads(v.decode("utf-8")),
    )
    session = requests.Session()
    
    # Create table if it doesn't exist
    schema = {
        "name": TABLE,
        "ColumnSchema": [{"name": "m"}]  # column family 'm' for metrics
    }
    r = session.put(f"{HBASE_REST}/{TABLE}/schema", 
                    json=schema,
                    headers={"Content-Type": "application/json"},
                    timeout=10)
    if r.status_code not in [200, 201]:
        print(f"Table creation result: {r.status_code} - {r.text}")
    else:
        print(f"Table {TABLE} created or already exists")
    
    print(f"Starting to consume from topics: {INPUT_TOPICS}")
    for msg in consumer:
        d = msg.value
        rk = rowkey(d)
        cells = to_cells(d)
        # Write to specific row using standard HBase REST API
        payload = {"Row":[{"key": b64(rk), "Cell": cells}]}
        url = f"{HBASE_REST}/{TABLE}/{rk}"
        r = session.put(url, 
                        json=payload,
                        headers={"Content-Type": "application/json", "Accept": "application/json"},
                        timeout=5)
        if r.status_code >= 300:
            print(f"HBase write error {r.status_code} to {url}: {r.text[:200]}")
        elif msg.offset % 100 == 0:  # Log every 100 messages
            print(f"Successfully wrote {msg.offset} messages to HBase")

if __name__ == "__main__":
    main()
