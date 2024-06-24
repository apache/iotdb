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

package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import org.apache.tsfile.file.metadata.IDeviceID;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class FetchDevice extends Statement {

  private final String database;

  private final String tableName;

  private final List<Object[]> deviceIdList;

  private transient List<IDeviceID> partitionKeyList;

  public FetchDevice(String database, String tableName, List<Object[]> deviceIdList) {
    super(null);
    this.database = database;
    this.tableName = tableName;
    this.deviceIdList = deviceIdList;
  }

  public String getDatabase() {
    return database;
  }

  public String getTableName() {
    return tableName;
  }

  public List<Object[]> getDeviceIdList() {
    return deviceIdList;
  }

  public List<IDeviceID> getPartitionKeyList() {
    if (partitionKeyList == null) {
      List<IDeviceID> partitionKeyList = new ArrayList<>();
      for (Object[] rawId : deviceIdList) {
        String[] partitionKey = new String[rawId.length + 1];
        partitionKey[0] = tableName;
        for (int i = 0; i < rawId.length; i++) {
          partitionKey[i + 1] = Objects.toString(rawId[i].toString());
        }
        partitionKeyList.add(IDeviceID.Factory.DEFAULT_FACTORY.create(partitionKey));
      }
      this.partitionKeyList = partitionKeyList;
    }
    return partitionKeyList;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitFetchDevice(this, context);
  }

  @Override
  public List<? extends Node> getChildren() {
    return null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    FetchDevice that = (FetchDevice) o;
    return Objects.equals(database, that.database)
        && Objects.equals(tableName, that.tableName)
        && Objects.equals(deviceIdList, that.deviceIdList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(database, tableName, deviceIdList);
  }

  @Override
  public String toString() {
    return "FetchDevice{"
        + "database='"
        + database
        + '\''
        + ", tableName='"
        + tableName
        + '\''
        + ", deviceIdList="
        + deviceIdList
        + '}';
  }
}
