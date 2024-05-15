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

package org.apache.iotdb.session;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;

import java.util.List;

public class RoundRobinPolicy implements QueryEndPointPolicy {

  private int index = 0;

  @Override
  public TEndPoint chooseOne(List<TEndPoint> endPointList) {
    int tmp = index;
    if (tmp >= endPointList.size()) {
      tmp = 0;
    }
    index = tmp + 1;
    return endPointList.get(tmp);
  }
}
