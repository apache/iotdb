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
package org.apache.iotdb.db.qp.physical.sys;

import org.apache.iotdb.tsfile.read.common.Path;

public class ShowTimeSeriesPlan extends ShowPlan {

  // path can be root, root.*  root.*.*.a etc.. if the wildcard is not at the tail, then each
  // * wildcard can only match one level, otherwise it can match to the tail.
  private Path path;
  private boolean isContains;
  private String key;
  private String value;
  private int limit;
  private int offset;

  public ShowTimeSeriesPlan(ShowContentType showContentType, Path path, boolean isContains,
      String key, String value, int limit, int offset) {
    super(showContentType);
    this.path = path;
    this.isContains = isContains;
    this.key = key;
    this.value = value;
    this.limit = limit;
    this.offset = offset;
  }

  public Path getPath() {
    return this.path;
  }

  public boolean isContains() {
    return isContains;
  }

  public String getKey() {
    return key;
  }

  public String getValue() {
    return value;
  }

  public int getLimit() {
    return limit;
  }

  public int getOffset() {
    return offset;
  }
}