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
package org.apache.iotdb.commons.path.fa.dfa.transition;

import org.apache.iotdb.commons.conf.IoTDBConstant;

import org.apache.commons.lang3.StringUtils;

import java.util.List;

public class DFAWildcardTransition extends AbstractDFATransition {
  private final List<String> rejectEventList;

  public DFAWildcardTransition(int index, List<String> rejectEventList) {
    super(index);
    this.rejectEventList = rejectEventList;
  }

  @Override
  public String toString() {
    if (rejectEventList == null || rejectEventList.isEmpty()) {
      return IoTDBConstant.ONE_LEVEL_PATH_WILDCARD;
    } else {
      return IoTDBConstant.ONE_LEVEL_PATH_WILDCARD
          + "/("
          + StringUtils.join(rejectEventList, ",")
          + ")";
    }
  }

  @Override
  public String getAcceptEvent() {
    return IoTDBConstant.ONE_LEVEL_PATH_WILDCARD;
  }

  @Override
  public boolean isMatch(String event) {
    return !rejectEventList.contains(event);
  }
}
