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

package org.apache.iotdb.tsfile.read.reader.series;

public class PaginationController {

  public static final PaginationController UNLIMITED_PAGINATION_CONTROLLER =
      new PaginationController(0L, 0L);

  private final boolean hasLimit;

  private long curLimit;
  private long curOffset;

  public PaginationController(long limit, long offset) {
    // row limit for result set. The default value is 0, which means no limit
    this.curLimit = limit;
    this.hasLimit = limit > 0;

    // row offset for result set. The default value is 0
    this.curOffset = offset;
  }

  public boolean hasCurOffset() {
    return curOffset > 0;
  }

  public boolean hasCurOffset(long rowCount) {
    return curOffset >= rowCount;
  }

  public boolean hasCurLimit() {
    return !hasLimit || curLimit > 0;
  }

  public void consumeOffset(long rowCount) {
    curOffset -= rowCount;
  }

  public void consumeOffset() {
    curOffset--;
  }

  public void consumeLimit() {
    if (hasLimit) {
      curLimit--;
    }
  }
}
