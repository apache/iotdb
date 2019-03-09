/**
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

package org.apache.iotdb.db.query.reader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.db.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class FakedIPointReader implements IPointReader{

  private Iterator<TimeValuePair> iterator;
  private boolean hasCachedTimeValuePair = false;
  private TimeValuePair cachedTimeValuePair;

  public FakedIPointReader(long startTime, int size, int interval, int modValue) {
    long time = startTime;
    List<TimeValuePair> list = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      list.add(
          new TimeValuePair(time, TsPrimitiveType.getByType(TSDataType.INT64, time % modValue)));
      time += interval;
    }
    iterator = list.iterator();
  }

  @Override
  public boolean hasNext() throws IOException {
    return hasCachedTimeValuePair || iterator.hasNext();
  }

  @Override
  public TimeValuePair next() throws IOException {
    if (hasCachedTimeValuePair){
      hasCachedTimeValuePair = false;
      return cachedTimeValuePair;
    }
    return iterator.next();
  }

  @Override
  public TimeValuePair current() throws IOException {
    if(hasCachedTimeValuePair){
      return cachedTimeValuePair;
    }
    cachedTimeValuePair = iterator.next();
    hasCachedTimeValuePair = true;
    return cachedTimeValuePair;
  }

  @Override
  public void close() throws IOException {

  }
}
