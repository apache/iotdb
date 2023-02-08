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

  private final boolean hasLimit;

  private int curLimit;
  private int curOffset;

  public PaginationController(int limit, int offset) {
    // row limit for result set. The default value is 0, which means no limit
    this.curLimit = limit;
    this.hasLimit = limit > 0;

    // row offset for result set. The default value is 0
    this.curOffset = offset;
  }

  public boolean hasCurOffset() {
    return curOffset > 0;
  }

  public boolean hasCurOffset(int rowCount) {
    return curOffset > rowCount;
  }

  public boolean hasCurLimit() {
    return !hasLimit || curLimit > 0;
  }

  public boolean hasCurLimit(int rowCount) {
    return !hasLimit || curLimit > rowCount;
  }

  public void consumeOffset(int rowCount) {
    curOffset -= rowCount;
  }

  public void consumeOffset() {
    curOffset--;
  }

  public void consumeLimit(int rowCount) {
    if (hasLimit) {
      curLimit -= rowCount;
    }
  }

  public void consumeLimit() {
    if (hasLimit) {
      curLimit--;
    }
  }
}
