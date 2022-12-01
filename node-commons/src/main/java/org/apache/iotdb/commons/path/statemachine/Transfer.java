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
package org.apache.iotdb.commons.path.statemachine;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.schema.tree.ITreeNode;

import org.apache.commons.lang3.StringUtils;

import java.util.List;

public class Transfer {

  private final String acceptName;
  private List<String> rejectList;

  public Transfer(String acceptName) {
    this.acceptName = acceptName;
  }

  public Transfer(String acceptName, List<String> rejectList) {
    this.acceptName = acceptName;
    this.rejectList = rejectList;
  }

  public String getKey() {
    return acceptName;
  }

  public boolean match(ITreeNode node) {
    // TODO
    return false;
  }

  public boolean isBatch() {
    return IoTDBConstant.ONE_LEVEL_PATH_WILDCARD.equals(acceptName);
  }

  @Override
  public String toString() {
    if (rejectList == null || rejectList.isEmpty()) {
      return acceptName;
    } else {
      return acceptName + "/(" + StringUtils.join(rejectList, ",") + ")";
    }
  }
}
