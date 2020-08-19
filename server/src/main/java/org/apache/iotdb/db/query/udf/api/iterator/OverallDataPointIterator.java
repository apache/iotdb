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

package org.apache.iotdb.db.query.udf.api.iterator;

import java.io.IOException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.tsfile.utils.Binary;

public interface OverallDataPointIterator extends Iterator {

  DataPointIterator getDataPointIterator();

  DataPointBatchIterator getSizeLimitedDataPointBatchIterator(final int batchSize)
      throws QueryProcessException;

  DataPointBatchIterator getSizeLimitedDataPointBatchIterator(final int batchSize,
      final long displayWindowBegin) throws QueryProcessException;

  DataPointBatchIterator getTimeWindowDataPointBatchIterator(final long timeInterval,
      final long slidingStep) throws QueryProcessException;

  DataPointBatchIterator getTimeWindowDataPointBatchIterator(final long displayWindowBegin,
      final long displayWindowEnd, final long timeInterval, final long slidingStep)
      throws QueryProcessException;

  int size();

  long getTime(int index) throws IOException;

  int getInt(int index) throws IOException;

  long getLong(int index) throws IOException;

  boolean getBoolean(int index) throws IOException;

  float getFloat(int index) throws IOException;

  double getDouble(int index) throws IOException;

  Binary getBinary(int index) throws IOException;

  String getString(int index) throws IOException;
}
