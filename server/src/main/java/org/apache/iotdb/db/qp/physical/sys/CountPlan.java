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

import org.apache.iotdb.db.metadata.PartialPath;

/**
 * CountPlan is used to count time-series and count nodes. COUNT_TIMESERIES if using "COUNT
 * TIMESERIES <Path>" and only this command supports wildcard. COUNT_NODE_TIMESERIES if using "COUNT
 * TIMESERIES <Path> GROUP BY LEVEL=<INTEGER>" COUNT_NODE if using "COUNT NODES <Path>
 * LEVEL=<INTEGER>"
 */
public class CountPlan extends ShowPlan {

  private PartialPath path;
  private int level;

  public CountPlan(ShowContentType showContentType, PartialPath path) {
    super(showContentType);
    this.path = path;
  }

  public CountPlan(ShowContentType showContentType, PartialPath path, int level) {
    super(showContentType);
    this.path = path;
    this.level = level;
  }

  public int getLevel() {
    return level;
  }

  @Override
  public PartialPath getPath() {
    return path;
  }
}
