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

package org.apache.iotdb.db.storageengine.dataregion.read.reader.common;

import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.reader.IPointReader;

import java.io.IOException;

public class NoDataPointReader implements IPointReader {

  private NoDataPointReader() {}

  private static final IPointReader instance = new NoDataPointReader();

  public static IPointReader getInstance() {
    return instance;
  }

  @Override
  public boolean hasNextTimeValuePair() throws IOException {
    return false;
  }

  @Override
  public TimeValuePair nextTimeValuePair() throws IOException {
    return null;
  }

  @Override
  public TimeValuePair currentTimeValuePair() throws IOException {
    return null;
  }

  @Override
  public long getUsedMemorySize() {
    return 0;
  }

  @Override
  public void close() throws IOException {}
}
