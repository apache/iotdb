/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.  You may obtain
 * a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied.  See the License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.query.dataset;

import java.io.IOException;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.db.utils.TimeValuePairUtils;
import org.apache.iotdb.tsfile.read.common.BatchData;

public class BatchDataPointReader implements IPointReader {

  private BatchData batchData;

  public BatchDataPointReader(BatchData batchData) {
    this.batchData = batchData;
  }

  @Override
  public boolean hasNext() throws IOException {
    return batchData.hasNext();
  }

  @Override
  public TimeValuePair next() throws IOException {
    TimeValuePair timeValuePair = TimeValuePairUtils.getCurrentTimeValuePair(batchData);
    batchData.next();
    return timeValuePair;
  }

  @Override
  public TimeValuePair current() throws IOException {
    TimeValuePair timeValuePair = TimeValuePairUtils.getCurrentTimeValuePair(batchData);
    return timeValuePair;
  }

  @Override
  public void close() throws IOException {
    //batch data doesn't need to close.
  }
}
