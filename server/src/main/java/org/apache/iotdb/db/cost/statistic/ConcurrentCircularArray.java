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

package org.apache.iotdb.db.cost.statistic;

public class ConcurrentCircularArray {

  long[] data;
  int tail;
  int head;

  public ConcurrentCircularArray(int size) {
    this.data = new long[size];
    tail = head = 0;
  }

  /**
   * @param d the data
   * @return true if successfully; false if there is no space.
   */
  public synchronized boolean put(long d) {
    if ((tail + 1) % data.length == head) {
      return false;
    }
    data[tail++] = d;
    tail = tail % data.length;
    return true;
  }

  public synchronized boolean hasData() {
    return tail != head;
  }

  /**
   * @return -1 if there is no data.(However, you should call hasData() frist to avoid returning -1)
   */
  public synchronized long take() {
    if (tail != head) {
      long result = data[head++];
      head = head % data.length;
      return result;
    } else {
      return -1;
    }
  }

  /** drop all of the elements in array. */
  public synchronized void clear() {
    tail = head = 0;
  }
}
