/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.influxdb.protocol.meta;

import org.apache.iotdb.influxdb.protocol.dto.SessionPoint;
import org.apache.iotdb.rpc.IoTDBConnectionException;

import java.util.HashMap;
import java.util.Map;

public class MetaManagerHolder {

  private static final Map<String, MetaManager> metaManagers = new HashMap<>();

  public static synchronized MetaManager getInstance(SessionPoint sessionPoint) {
    MetaManager metaManager;
    if (metaManagers.containsKey(sessionPoint.ipAndPortToString())) {
      metaManager = metaManagers.get(sessionPoint.ipAndPortToString());
    } else {
      metaManager = new MetaManager(sessionPoint);
      metaManagers.put(sessionPoint.ipAndPortToString(), metaManager);
    }
    metaManager.increaseReference();
    return metaManager;
  }

  public static synchronized void close(String meteManagersKey) throws IoTDBConnectionException {
    if (metaManagers.containsKey(meteManagersKey)) {
      MetaManager metaManager = metaManagers.get(meteManagersKey);
      metaManager.decreaseReference();
      if (metaManager.hasNoReference()) {
        metaManager.close();
        metaManagers.remove(meteManagersKey);
      }
    }
  }
}
