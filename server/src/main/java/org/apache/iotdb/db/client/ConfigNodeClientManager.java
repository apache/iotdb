/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.client;

import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.consensus.ConfigRegionId;

public class ConfigNodeClientManager {
  private static final class ConfigNodeClientManagerHolder {
    private static final IClientManager<ConfigRegionId, ConfigNodeClient> INSTANCE =
        new IClientManager.Factory<ConfigRegionId, ConfigNodeClient>()
            .createClientManager(new DataNodeClientPoolFactory.ConfigNodeClientPoolFactory());
  }

  public static IClientManager<ConfigRegionId, ConfigNodeClient> getInstance() {
    return ConfigNodeClientManagerHolder.INSTANCE;
  }
}
