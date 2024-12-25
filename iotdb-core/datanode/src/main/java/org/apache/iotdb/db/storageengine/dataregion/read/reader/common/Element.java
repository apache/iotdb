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

public class Element {

  private final MergeReaderPriority priority;
  private final IPointReader reader;
  private TimeValuePair timeValuePair;

  public Element(IPointReader reader, TimeValuePair timeValuePair, MergeReaderPriority priority) {
    this.reader = reader;
    this.timeValuePair = timeValuePair;
    this.priority = priority;
  }

  public long currTime() {
    return timeValuePair.getTimestamp();
  }

  public TimeValuePair currPair() {
    return timeValuePair;
  }

  public boolean hasNext() throws IOException {
    return reader.hasNextTimeValuePair();
  }

  public void next() throws IOException {
    timeValuePair = reader.nextTimeValuePair();
  }

  public void close() throws IOException {
    reader.close();
  }

  public IPointReader getReader() {
    return reader;
  }

  public TimeValuePair getTimeValuePair() {
    return timeValuePair;
  }

  public void setTimeValuePair(TimeValuePair timeValuePair) {
    this.timeValuePair = timeValuePair;
  }

  public MergeReaderPriority getPriority() {
    return priority;
  }
}
