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

package org.apache.iotdb.db.query.udf.api.customizer.config;

import java.time.ZoneId;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.AccessStrategy;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.SlidingTimeWindowAccessStrategy;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class UDTFConfigurations extends UDFConfigurations {

  protected final ZoneId zoneId;

  public UDTFConfigurations(ZoneId zoneId) {
    this.zoneId = zoneId;
  }

  public UDTFConfigurations setOutputDataType(TSDataType outputDataType) {
    this.outputDataType = outputDataType;
    return this;
  }

  protected AccessStrategy accessStrategy;

  public AccessStrategy getAccessStrategy() {
    return accessStrategy;
  }

  public UDTFConfigurations setAccessStrategy(AccessStrategy accessStrategy) {
    this.accessStrategy = accessStrategy;
    if (accessStrategy instanceof SlidingTimeWindowAccessStrategy
        && ((SlidingTimeWindowAccessStrategy) accessStrategy).getZoneId() == null) {
      ((SlidingTimeWindowAccessStrategy) accessStrategy).setZoneId(zoneId);
    }
    return this;
  }

  @Override
  public void check() throws QueryProcessException {
    super.check();
    if (accessStrategy == null) {
      throw new QueryProcessException("Access strategy is not set.");
    }
    accessStrategy.check();
  }
}
