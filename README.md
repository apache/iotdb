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
    "AS IS" BAstepSIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

- The code for the sampling method based on minimal areal difference and other baselines compared in the experiments are available in the [org.apache.iotdb.db.query.eBUG](https://github.com/apache/iotdb/tree/research/area-visualization/server/src/main/java/org/apache/iotdb/db/query/eBUG) repository.
- The README of [Apache IoTDB](https://iotdb.apache.org/) itself is in [README_IOTDB.md](README_IOTDB.md). To build this repository, run `mvn clean package -DskipTests -pl -distribution`.

A python usage example of performing online sampling based on minimal areal difference:
```python
m=100000
sql_online="select EBUG(s1,'m'='{}','e'='3373465') from root.sg.d5".format(m)
sql=sql_online

ip = "127.0.0.1"
port_ = "6667"
username_ = "root"
password_ = "root"
fetchsize = 100000
session = Session(ip, port_, username_, password_, fetchsize)
session.open(False)

# query:
result = session.execute_query_statement(sql)

start = time.time_ns()
df = result.todf()

print(df)

t_online=df.iloc[:,0]
v_online=df.iloc[:,1]

# interactive visualization:
fig = go.Figure()
fig.add_trace(go.Scatter(x=t_online, y=v_online))
fig.update_layout(
    autosize=False,
    width=850,
    height=650,
)
fig.show()
```

A python usage example of querying the first m rows of the precomputed time series $T_{pre}$:
```python
m=100000
sql_pre="select pre_t,pre_v from root.sg.d6 limit {}".format(m)
sql=sql_pre

ip = "127.0.0.1"
port_ = "6667"
username_ = "root"
password_ = "root"
fetchsize = 100000
session = Session(ip, port_, username_, password_, fetchsize)
session.open(False)

# query:
result = session.execute_query_statement(sql)

start = time.time_ns() 
df = result.todf() 

print(df)

df = df.rename(columns={'root.sg.d6.pre_t': 't', 'root.sg.d6.pre_v': 'v'})
df = df.sort_values(by='t')
t_pre=df['t']
v_pre=df['v']

# interactive visualization:
fig = go.Figure()
fig.add_trace(go.Scatter(x=t_pre, y=v_pre))
fig.update_layout(
    autosize=False,
    width=850,
    height=650,
)
fig.show()
```
