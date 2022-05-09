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

import org.apache.iotdb.db.query.reader.universal.DescPriorityMergeReader;
import org.apache.iotdb.db.query.reader.universal.Element;

import java.util.PriorityQueue;

/**
 * This class extends {@link extends DescPriorityMergeReader} for data sources with different
 * priorities.
 */
public class AssignPathDescPriorityMergeReader extends DescPriorityMergeReader
    implements IAssignPathPriorityMergeReader {

  private String fullPath;

  public AssignPathDescPriorityMergeReader(String fullPath) {
    super();
    this.fullPath = fullPath;
  }

  @Override
  public PriorityQueue<Element> getHeap() {
    return heap;
  }

  @Override
  public String getFullPath() {
    return fullPath;
  }
}
