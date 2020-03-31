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

package org.apache.iotdb.cluster.query.reader;

import java.io.IOException;
import java.util.List;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;

public class MergedReaderByTime implements IReaderByTimestamp {

  private List<IReaderByTimestamp> innerReaders;
  private Object[][] valueCaches;

  public MergedReaderByTime(
      List<IReaderByTimestamp> innerReaders) {
    this.innerReaders = innerReaders;
    valueCaches = new Object[innerReaders.size()][];
  }

  @Override
  public Object[] getValuesInTimestamps(long[] timestamps) throws IOException {
    Object[] rst = new Object[timestamps.length];
    for (int i = 0; i < innerReaders.size(); i++) {
      if (innerReaders.get(i) != null) {
        valueCaches[i] = innerReaders.get(i).getValuesInTimestamps(timestamps);
      }
    }
    forTime:
    for (int i = 0; i < rst.length; i++) {
      for (int j = 0; j < innerReaders.size(); j++) {
        if (valueCaches[j] != null && valueCaches[j][i] != null) {
          rst[i] = valueCaches[j][i];
          continue forTime;
        }
      }
    }
    return rst;
  }
}
