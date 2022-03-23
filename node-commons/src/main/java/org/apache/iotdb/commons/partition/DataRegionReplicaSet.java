/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.commons.partition;

import java.util.List;
import org.apache.iotdb.service.rpc.thrift.EndPoint;

public class DataRegionReplicaSet {
  private DataRegionId Id;
  private List<EndPoint> endPointList;

  public DataRegionReplicaSet(DataRegionId Id, List<EndPoint> endPointList) {
    this.Id = Id;
    this.endPointList = endPointList;
  }

  public List<EndPoint> getEndPointList() {
    return endPointList;
  }

  public void setEndPointList(List<EndPoint> endPointList) {
    this.endPointList = endPointList;
  }

  public DataRegionId getId() {
    return Id;
  }

  public void setId(DataRegionId id) {
    this.Id = id;
  }

  public String toString() {
    return String.format("%s:%s", Id, endPointList);
  }
}
