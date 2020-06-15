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

package org.apache.iotdb.db.utils;

import java.util.Arrays;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

/**
 * ComposedStatus consists of a list of TSStatus. This is to make batch insertion have compatible
 * interfaces as other plans.
 */
public class ComposedStatus extends TSStatus {
  private TSStatus[] statusList;

  public ComposedStatus(TSStatus[] statusList) {
    super(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
    this.statusList = statusList;
  }

  public TSStatus[] getStatusList() {
    return statusList;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    ComposedStatus that = (ComposedStatus) o;
    return Arrays.equals(statusList, that.statusList);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + Arrays.hashCode(statusList);
    return result;
  }
}
