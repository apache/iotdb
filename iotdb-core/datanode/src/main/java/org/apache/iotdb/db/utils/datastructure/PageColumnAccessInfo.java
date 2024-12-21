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

package org.apache.iotdb.db.utils.datastructure;

import static org.apache.iotdb.db.storageengine.dataregion.memtable.IWritableMemChunk.MAX_NUMBER_OF_POINTS_IN_PAGE;

public class PageColumnAccessInfo {
  // time -> (selectedTVList, rowIndex)
  private final int[][] accessIndices;
  private int count;

  public PageColumnAccessInfo() {
    this.accessIndices = new int[MAX_NUMBER_OF_POINTS_IN_PAGE][];
    this.count = 0;
  }

  public int[] get(int index) {
    return accessIndices[index];
  }

  public void add(int[] columnAccess) {
    accessIndices[count++] = columnAccess;
  }

  public int count() {
    return count;
  }

  public void reset() {
    count = 0;
  }
}
