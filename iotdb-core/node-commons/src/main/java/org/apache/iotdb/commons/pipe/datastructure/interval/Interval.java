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

package org.apache.iotdb.commons.pipe.datastructure.interval;

public class Interval<T extends Interval<T>> implements Comparable<Interval<?>> {
  public long start;
  public long end;

  public Interval(final long start, final long end) {
    this.start = start;
    this.end = end;
  }

  public void onMerged(final T another) {
    // Do nothing by default
  }

  public void onRemoved() {
    // Do nothing by default
  }

  @Override
  public int compareTo(final Interval other) {
    return Long.compare(this.start, other.start);
  }

  @Override
  public String toString() {
    return "[" + start + ", " + end + "]";
  }
}
