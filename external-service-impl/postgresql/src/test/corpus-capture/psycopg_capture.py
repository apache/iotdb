#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
import sys, datetime, psycopg
pw = open(sys.argv[1]).read().strip()
dsn = f"host=127.0.0.1 port=54314 dbname=iotdb user=postgres password={pw} application_name=psycopg"
with psycopg.connect(dsn) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT version()"); cur.fetchall()
        cur.execute("SELECT * FROM refdb.sensors LIMIT 5"); cur.fetchall()
        for m in ("m1", "m2", "m3", "m4", "m5", "m1"):  # prepared after 5 executions
            cur.execute('SELECT "time", temperature FROM refdb.sensors WHERE machine = %s AND "time" >= %s LIMIT %s',
                        (m, datetime.datetime(2026, 10, 25, tzinfo=datetime.timezone.utc), 10))
            cur.fetchall()
        cur.execute("SELECT * FROM refdb.sensors WHERE running = %b AND level > %s LIMIT 3", (True, 50), binary=True); cur.fetchall()
    with conn.cursor(name="big") as cur:  # server-side cursor
        cur.itersize = 5000
        cur.execute("SELECT * FROM refdb.events")
        n = sum(1 for _ in cur)
    conn.commit()
print("ok", n)
