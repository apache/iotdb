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

# IoTDB subscription examples

These Java examples require an IoTDB server with subscriptions enabled. In the
Apache IoTDB 2.0.12 binary distribution, creating a topic fails with
`1912: Subscription is not enabled`. The server's subscription switch is set
at build time and is unavailable in `iotdb-system.properties` in that release.

- Tree model examples. [SubscriptionSessionExample](src/main/java/org/apache/iotdb/SubscriptionSessionExample.java)
  and [ConsensusSubscriptionSessionExample](src/main/java/org/apache/iotdb/ConsensusSubscriptionSessionExample.java).
- Table model examples. [TableModelSubscriptionSessionExample](src/main/java/org/apache/iotdb/TableModelSubscriptionSessionExample.java)
  and [ConsensusTableModelSubscriptionSessionExample](src/main/java/org/apache/iotdb/ConsensusTableModelSubscriptionSessionExample.java).
