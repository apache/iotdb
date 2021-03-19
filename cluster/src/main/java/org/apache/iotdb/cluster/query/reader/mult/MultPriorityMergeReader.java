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

import org.apache.iotdb.db.query.reader.universal.PriorityMergeReader;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.reader.IPointReader;

import java.io.IOException;
import java.util.Set;

/** This class implements {@link IPointReader} for data sources with different priorities. */
public class MultPriorityMergeReader extends PriorityMergeReader implements IMultPointReader {

  private String fullPath;

  public MultPriorityMergeReader(String fullPath) {
    super();
    this.fullPath = fullPath;
  }

  public void addReader(IMultPointReader reader, long priority) throws IOException {
    if (reader.hasNextTimeValuePair(fullPath)) {
      heap.add(
          new MultElement(
              reader, reader.nextTimeValuePair(fullPath), new MergeReaderPriority(priority, 0)));
    } else {
      reader.close();
    }
  }

  @Override
  public boolean hasNextTimeValuePair(String fullPath) throws IOException {
    return false;
  }

  @Override
  public TimeValuePair nextTimeValuePair(String fullPath) throws IOException {
    return null;
  }

  @Override
  public Set<String> getAllPaths() {
    return null;
  }

  class MultElement extends Element {

    IMultPointReader reader;
    TimeValuePair timeValuePair;
    MergeReaderPriority priority;

    MultElement(
        IMultPointReader reader, TimeValuePair timeValuePair, MergeReaderPriority priority) {
      super(reader, timeValuePair, priority);
      this.reader = reader;
      this.timeValuePair = timeValuePair;
      this.priority = priority;
    }

    @Override
    public boolean hasNext() throws IOException {
      return reader.hasNextTimeValuePair(fullPath);
    }

    @Override
    public void next() throws IOException {
      timeValuePair = reader.nextTimeValuePair(fullPath);
    }
  }
}
