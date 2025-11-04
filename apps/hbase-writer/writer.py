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
    for msg in consumer:
        d = msg.value
        rk = rowkey(d)
        payload = {"Row":[{"key": b64(rk), "Cell": to_cells(d)}]}
        # Stargate bulk PUT; many builds accept JSON at /<table>/<row>
        r = session.put(f"{HBASE_REST}/{TABLE}/fakerow", json=payload, timeout=5)
        if r.status_code >= 300:
            print("HBase write error", r.status_code, r.text)

if __name__ == "__main__":
    main()
