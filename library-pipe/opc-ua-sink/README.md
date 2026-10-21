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

# OPC UA Sink Plugin

`opc-ua-sink` is an external Pipe Sink plugin that publishes IoTDB data through OPC UA or writes it to an external OPC UA server. Eclipse Milo and the plugin's other runtime dependencies are bundled in its fat JAR instead of the default IoTDB server package.

## Build

Run from the repository root:

```bash
mvn clean package -pl library-pipe/opc-ua-sink -am -DskipTests
```

The plugin artifact is generated at:

`library-pipe/opc-ua-sink/target/opc-ua-sink-<version>-jar-with-dependencies.jar`

It is also included under `ext/pipe` in the standalone `library-pipe` distribution ZIP.

## Register

The fat JAR is larger than the default 16 MiB ConfigNode consensus limit. Before starting the
nodes, set the following properties in `iotdb-system.properties` so the plugin can be transferred
and replicated:

```properties
config_node_ratis_log_appender_buffer_size_max=33554432
dn_thrift_max_frame_size=33554432
```

Upload the fat JAR to a URI accessible to IoTDB and register the plugin before creating an OPC UA Pipe:

```sql
CREATE PIPEPLUGIN `opc-ua-sink`
AS 'org.apache.iotdb.pipe.plugin.sink.opcua.OpcUaSink'
USING URI 'file:///path/to/opc-ua-sink-<version>-jar-with-dependencies.jar';
```

`opc-ua-connector` is the legacy-compatible alias. Register it separately when existing Pipe definitions use that name:

```sql
CREATE PIPEPLUGIN `opc-ua-connector`
AS 'org.apache.iotdb.pipe.plugin.sink.opcua.OpcUaSink'
USING URI 'file:///path/to/opc-ua-sink-<version>-jar-with-dependencies.jar';
```

## Example

The following Pipe starts an OPC UA server with no message security:

```sql
CREATE PIPE opc_ua_pipe
WITH SINK (
  'sink' = 'opc-ua-sink',
  'sink.opcua.model' = 'client-server',
  'sink.opcua.security-policy' = 'NONE',
  'sink.opcua.tcp.port' = '12686',
  'sink.opcua.https.port' = '8443'
);
```

The client example is located in `library-pipe/opc-ua-sink-example`.
