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
package org.apache.iotdb.confignode.partition;

import org.apache.iotdb.consensus.common.Endpoint;

import java.util.List;

public class SchemaRegionReplicaSet {
  private int schemaRegionId;
  private List<Endpoint> endPointList;

  public int getSchemaRegionId() {
    return schemaRegionId;
  }

  public void setSchemaRegionId(int schemaRegionId) {
    this.schemaRegionId = schemaRegionId;
  }

  public List<Endpoint> getEndPointList() {
    return endPointList;
  }

  public void setEndPointList(List<Endpoint> endPointList) {
    this.endPointList = endPointList;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("SchemaRegionReplicaSet {");
    sb.append("schemaRegionId = ").append(schemaRegionId);
    endPointList.forEach(
        endpoint -> {
          sb.append(", EndPoint = ").append(endpoint);
        });
    sb.append("}");
    return sb.toString();
  }
}
