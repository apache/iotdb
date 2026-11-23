<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->
# IoTDB-MQTT-Broker Example

## Function
```
The example is to show how to send data to IoTDB from a mqtt client.
```

## Usage

* Update configuration to enable MQTT service. (`enable_mqtt_service=true` in iotdb-datanode.properties)
* Launch the IoTDB server.
* Setup database `CREATE DATABASE root.sg` and create time timeseries `CREATE TIMESERIES root.sg.d1.s1 WITH DATATYPE=DOUBLE, ENCODING=PLAIN`.
* Run `org.apache.iotdb.mqtt.MQTTClient` to run the mqtt client and send events to server.

## Python client (tree model)

Set `enable_mqtt_service=true` and `mqtt_payload_formatter=json` in
`conf/iotdb-system.properties`, then start the local IoTDB instance.

Create the destination in an IoTDB CLI session using the tree SQL dialect.

```sql
CREATE DATABASE root.sg;
CREATE TIMESERIES root.sg.d1.s1 WITH DATATYPE=DOUBLE, ENCODING=PLAIN;
```

Install the MQTT client with `python -m pip install paho-mqtt`. Publish a
reading to the local MQTT port. Set `IOTDB_USER` and `IOTDB_PASSWORD` in
your shell to the credentials you use in the IoTDB CLI. Keep the MQTT client ID
non-empty. The MQTT account needs `WRITE_DATA` on `root.sg.d1.s1`.

```python
import json
import time
from os import environ
from uuid import uuid4

from paho.mqtt.publish import single

reading = {
    "device": "root.sg.d1",
    "timestamp": time.time_ns() // 1_000_000,
    "measurements": ["s1"],
    "values": [37.5],
}
print(reading["timestamp"])

single(
    "root.sg.d1.s1",
    payload=json.dumps(reading),
    hostname="127.0.0.1",
    port=1883,
    client_id=f"iotdb-{uuid4().hex[:12]}",
    qos=1,
    auth={"username": environ["IOTDB_USER"], "password": environ["IOTDB_PASSWORD"]},
)
```

Replace `<timestamp>` with the printed timestamp, then run this CLI query:

```sql
SELECT s1 FROM root.sg.d1 WHERE time = <timestamp>;
```

Check that this row contains `37.5`. If IoTDB is still processing the message,
run the same query again. Returning from `single()` does not confirm that IoTDB
stored the reading; filtering by timestamp avoids reading an earlier value.
