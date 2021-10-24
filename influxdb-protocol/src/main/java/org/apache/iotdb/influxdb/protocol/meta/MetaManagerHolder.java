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

import java.util.HashMap;
import java.util.Map;

public class MetaManagerHolder {
    private static final Map<SessionPoint,MetaManager> meteManagers=new HashMap<>();

    public static MetaManager getInstance(SessionPoint sessionPoint) {
        if (meteManagers.containsKey(sessionPoint)){
            return meteManagers.get(sessionPoint);
        }
        else {
            meteManagers.put(sessionPoint,MetaManager.getInstance(sessionPoint));
            return MetaManager.getInstance(sessionPoint);
        }
    }
}
