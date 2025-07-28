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

package org.apache.iotdb.db.utils.columngenerator.parameter;

import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.utils.columngenerator.ColumnGeneratorType;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

public class SlidingTimeColumnGeneratorParameter extends ColumnGeneratorParameter {
  private final boolean ascending;
  private final GroupByTimeParameter groupByTimeParameter;

  public SlidingTimeColumnGeneratorParameter(
      GroupByTimeParameter groupByTimeParameter, boolean ascending) {
    super(ColumnGeneratorType.SLIDING_TIME);
    this.groupByTimeParameter = groupByTimeParameter;
    this.ascending = ascending;
  }

  public GroupByTimeParameter getGroupByTimeParameter() {
    return groupByTimeParameter;
  }

  public boolean isAscending() {
    return ascending;
  }

  @Override
  protected void serializeAttributes(ByteBuffer buffer) {
    groupByTimeParameter.serialize(buffer);
    ReadWriteIOUtils.write(ascending, buffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    groupByTimeParameter.serialize(stream);
    ReadWriteIOUtils.write(ascending, stream);
  }

  @Override
  public List<String> getColumnNames() {
    return Collections.singletonList(ColumnHeaderConstant.ENDTIME);
  }

  @Override
  public List<TSDataType> getColumnTypes() {
    return Collections.singletonList(TSDataType.INT64);
  }

  public static SlidingTimeColumnGeneratorParameter deserialize(ByteBuffer byteBuffer) {
    GroupByTimeParameter groupByTimeParameter = GroupByTimeParameter.deserialize(byteBuffer);
    boolean ascending = ReadWriteIOUtils.readBool(byteBuffer);
    return new SlidingTimeColumnGeneratorParameter(groupByTimeParameter, ascending);
  }
}
