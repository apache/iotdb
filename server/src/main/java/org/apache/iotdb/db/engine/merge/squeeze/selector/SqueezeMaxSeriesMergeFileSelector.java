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

package org.apache.iotdb.db.engine.merge.squeeze.selector;

import org.apache.iotdb.db.engine.merge.BaseFileSelector;
import org.apache.iotdb.db.engine.merge.IMergeFileSelector;
import org.apache.iotdb.db.engine.merge.MaxSeriesMergeFileSelector;

/**
 * MaxSeriesMergeFileSelector is an extension of IMergeFileSelector which tries to maximize the
 * number of timeseries that can be merged at the same time.
 */
public class SqueezeMaxSeriesMergeFileSelector extends MaxSeriesMergeFileSelector {

  public SqueezeMaxSeriesMergeFileSelector(BaseFileSelector baseSelector) {
    super(baseSelector);
  }

}
