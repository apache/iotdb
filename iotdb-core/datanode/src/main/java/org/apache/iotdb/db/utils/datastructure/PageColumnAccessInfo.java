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

public class PageColumnAccessInfo {
  // time -> (selectedTVList, selectedIndex)
  private final int[][] indices;
  private int count;

  public PageColumnAccessInfo(int maxNumberOfPointsInPage) {
    this.indices = new int[maxNumberOfPointsInPage][];
    for (int i = 0; i < maxNumberOfPointsInPage; i++) {
      indices[i] = new int[2];
    }
    this.count = 0;
  }

  public int[] get(int index) {
    return indices[index];
  }

  public void add(int[] columnAccess) {
    indices[count][0] = columnAccess[0];
    indices[count][1] = columnAccess[1];
    count++;
  }

  public int count() {
    return count;
  }

  public void reset() {
    count = 0;
  }
}
