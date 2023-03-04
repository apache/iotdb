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

/** a special Element implementation when querying data from multi readers */
public class MultElement extends Element {
  private final String fullPath;

  public MultElement(
      String fullPath,
      AbstractMultPointReader reader,
      TimeValuePair timeValuePair,
      PriorityMergeReader.MergeReaderPriority priority) {
    super(reader, timeValuePair, priority);
    this.fullPath = fullPath;
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
