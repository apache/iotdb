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

import org.apache.iotdb.db.query.reader.universal.Element;
import org.apache.iotdb.db.query.reader.universal.PriorityMergeReader;
import org.apache.iotdb.tsfile.read.TimeValuePair;

import java.io.IOException;

/**
 * This class extends {@link extends PriorityMergeReader} for data sources with different
 * priorities.
 */
public class AssignPathPriorityMergeReader extends PriorityMergeReader {

  private String fullPath;

  public AssignPathPriorityMergeReader(String fullPath) {
    super();
    this.fullPath = fullPath;
  }

  public void addReader(AbstractMultPointReader reader, long priority) throws IOException {
    if (reader.hasNextTimeValuePair(fullPath)) {
      heap.add(
          new MultElement(
              reader, reader.nextTimeValuePair(fullPath), new MergeReaderPriority(priority, 0)));
    } else {
      reader.close();
    }
  }

  public class MultElement extends Element {
    public MultElement(
        AbstractMultPointReader reader, TimeValuePair timeValuePair, MergeReaderPriority priority) {
      super(reader, timeValuePair, priority);
    }

    @Override
    public boolean hasNext() throws IOException {
      return ((AbstractMultPointReader) reader).hasNextTimeValuePair(fullPath);
    }

    @Override
    public void next() throws IOException {
      timeValuePair = ((AbstractMultPointReader) reader).nextTimeValuePair(fullPath);
    }
  }
}
