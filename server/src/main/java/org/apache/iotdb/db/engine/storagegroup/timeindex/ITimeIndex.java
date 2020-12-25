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

package org.apache.iotdb.db.engine.storagegroup.timeindex;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.Set;
import org.apache.iotdb.db.exception.PartitionViolationException;
import org.apache.iotdb.db.rescon.CachedStringPool;

public interface ITimeIndex {

  Map<String, String> cachedDevicePool = CachedStringPool.getInstance()
      .getCachedPool();

  void init();

  void serialize(OutputStream outputStream) throws IOException;

  ITimeIndex deserialize(InputStream inputStream) throws IOException;

  void close();

  Set<String> getDevices();

  boolean endTimeEmpty();

  boolean stillLives(long timeLowerBound);

  long calculateRamSize();

  long estimateRamIncrement(String deviceToBeChecked);

  long getTimePartition(String file);

  long getTimePartitionWithCheck(String file) throws PartitionViolationException;

  void updateStartTime(String deviceId, long startTime);

  void updateEndTime(String deviceId, long endTime);

  long getStartTime(String deviceId);

  long getEndTime(String deviceId);
}
