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
package org.apache.iotdb.cluster.query.reader.mult;

import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.reader.IPointReader;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/** mult reader of local partition group */
public class MultSeriesRawDataPointReader extends AbstractMultPointReader {
  private Map<String, IPointReader> partitalPathReaders;

  public MultSeriesRawDataPointReader(Map<String, IPointReader> partitalPathReaders) {
    this.partitalPathReaders = partitalPathReaders;
  }

  @Override
  public boolean hasNextTimeValuePair(String fullPath) throws IOException {
    IPointReader seriesRawDataPointReader = partitalPathReaders.get(fullPath);
    return seriesRawDataPointReader.hasNextTimeValuePair();
  }

  @Override
  public TimeValuePair nextTimeValuePair(String fullPath) throws IOException {
    IPointReader seriesRawDataPointReader = partitalPathReaders.get(fullPath);
    return seriesRawDataPointReader.nextTimeValuePair();
  }

  @Override
  public Set<String> getAllPaths() {
    return partitalPathReaders.keySet();
  }

  @Override
  public void close() {}
}
