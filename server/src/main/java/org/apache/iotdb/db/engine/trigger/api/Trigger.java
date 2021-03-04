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

package org.apache.iotdb.db.engine.trigger.api;

import org.apache.iotdb.tsfile.utils.Binary;

public interface Trigger {

  @SuppressWarnings("squid:S112")
  default void onStart(TriggerAttributes attributes) throws Exception {}

  @SuppressWarnings("squid:S112")
  default void onStop() throws Exception {}

  @SuppressWarnings("squid:S112")
  default Integer fire(long time, Integer value) throws Exception {
    return value;
  }

  @SuppressWarnings("squid:S112")
  default Long fire(long time, Long value) throws Exception {
    return value;
  }

  @SuppressWarnings("squid:S112")
  default Float fire(long time, Float value) throws Exception {
    return value;
  }

  @SuppressWarnings("squid:S112")
  default Double fire(long time, Double value) throws Exception {
    return value;
  }

  @SuppressWarnings("squid:S112")
  default Boolean fire(long time, Boolean value) throws Exception {
    return value;
  }

  @SuppressWarnings("squid:S112")
  default Binary fire(long time, Binary value) throws Exception {
    return value;
  }
}
