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
package org.apache.iotdb.commons.path.dfa;

import org.apache.iotdb.commons.conf.IoTDBConstant;

import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Objects;

public class DFATransition implements IFATransition {

  private final String acceptEvent;
  private List<String> rejectEventList;

  public DFATransition(String acceptEvent) {
    this.acceptEvent = acceptEvent;
  }

  public DFATransition(String acceptEvent, List<String> rejectEventList) {
    this.acceptEvent = acceptEvent;
    this.rejectEventList = rejectEventList;
  }

  public String getAcceptEvent() {
    return acceptEvent;
  }

  public List<String> getRejectEventList() {
    return rejectEventList;
  }

  @Override
  public String getValue() {
    return acceptEvent;
  }

  @Override
  public boolean isMatch(String event) {
    if (isBatch()) {
      return !rejectEventList.contains(event);
    } else {
      return acceptEvent.equals(event);
    }
  }

  @Override
  public boolean isBatch() {
    return IoTDBConstant.ONE_LEVEL_PATH_WILDCARD.equals(acceptEvent);
  }

  @Override
  public String toString() {
    if (rejectEventList == null || rejectEventList.isEmpty()) {
      return acceptEvent;
    } else {
      return acceptEvent + "/(" + StringUtils.join(rejectEventList, ",") + ")";
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DFATransition that = (DFATransition) o;
    return Objects.equals(acceptEvent, that.acceptEvent)
        && Objects.equals(rejectEventList, that.rejectEventList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(acceptEvent, rejectEventList);
  }
}
