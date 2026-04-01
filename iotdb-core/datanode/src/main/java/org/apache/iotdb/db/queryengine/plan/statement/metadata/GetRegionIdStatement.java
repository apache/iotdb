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

package org.apache.iotdb.db.queryengine.plan.statement.metadata;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.statement.IConfigStatement;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * GET REGIONID statement
 *
 * <p>Here is the syntax definition:
 *
 * <p>SHOW (DATA|SCHEMA) REGIONID OF path=prefixPath WHERE (SERIESSLOTID operator_eq
 * seriesSlot=INTEGER_LITERAL|DEVICEID operator_eq deviceId=prefixPath) (OPERATOR_AND (TIMESLOTID
 * operator_eq timeSlot=INTEGER_LITERAL| TIMESTAMP operator_eq timeStamp=INTEGER_LITERAL))?
 */
public class GetRegionIdStatement extends Statement implements IConfigStatement {

  private String database;

  private IDeviceID device;
  private final TConsensusGroupType partitionType;
  private long startTimeStamp;

  private long endTimeStamp;

  public long getEndTimeStamp() {
    return endTimeStamp;
  }

  public void setEndTimeStamp(long endTimeStamp) {
    this.endTimeStamp = endTimeStamp;
  }

  public long getStartTimeStamp() {
    return startTimeStamp;
  }

  public void setStartTimeStamp(long startTimeStamp) {
    this.startTimeStamp = startTimeStamp;
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(GetRegionIdStatement.class);

  public GetRegionIdStatement(TConsensusGroupType partitionType) {
    super();
    this.partitionType = partitionType;
  }

  public String getDatabase() {
    return database;
  }

  public TConsensusGroupType getPartitionType() {
    return partitionType;
  }

  public IDeviceID getDevice() {
    return device;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public void setDevice(IDeviceID device) {
    this.device = device;
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitGetRegionId(this, context);
  }

  @Override
  public QueryType getQueryType() {
    return QueryType.READ;
  }

  @Override
  public List<PartialPath> getPaths() {
    if (database == null) {
      return new ArrayList<>();
    }
    try {
      return Collections.singletonList(new PartialPath(database));
    } catch (IllegalPathException e) {
      LOGGER.warn("illegal path: {}", database);
      return new ArrayList<>();
    }
  }
}
