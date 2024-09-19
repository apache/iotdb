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

package org.apache.iotdb.rpc;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class RedirectException extends IOException {

  private final TEndPoint endPoint;

  private final Map<String, TEndPoint> deviceEndPointMap;
  private final List<TEndPoint> deviceEndPointList;

  public RedirectException(TEndPoint endPoint) {
    super("later request in same group will be redirected to " + endPoint.toString());
    this.endPoint = endPoint;
    this.deviceEndPointMap = null;
    this.deviceEndPointList = null;
  }

  public RedirectException(Map<String, TEndPoint> deviceEndPointMap) {
    super("later request in same group will be redirected to " + deviceEndPointMap);
    this.endPoint = null;
    this.deviceEndPointMap = deviceEndPointMap;
    this.deviceEndPointList = null;
  }

  public RedirectException(List<TEndPoint> deviceEndPointList) {
    super("later request in same group will be redirected to " + deviceEndPointList);
    this.endPoint = null;
    this.deviceEndPointMap = null;
    this.deviceEndPointList = deviceEndPointList;
  }

  public TEndPoint getEndPoint() {
    return this.endPoint;
  }

  public Map<String, TEndPoint> getDeviceEndPointMap() {
    return deviceEndPointMap;
  }

  public List<TEndPoint> getDeviceEndPointList() {
    return deviceEndPointList;
  }
}
