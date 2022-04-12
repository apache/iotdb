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
package org.apache.iotdb.db.mpp.common.filter;

import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.path.PathDeserializeUtil;
import org.apache.iotdb.db.mpp.sql.constant.FilterConstant.FilterType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This class presents series condition which is general(e.g. numerical comparison) or defined by
 * user. Function is used for bottom operator.<br>
 * FunctionFilter has a {@code seriesPath}, and other filter condition.
 */
public class FunctionFilter extends QueryFilter {

  private static final Logger logger = LoggerFactory.getLogger(FunctionFilter.class);

  public FunctionFilter(FilterType filterType) {
    super(filterType);
  }

  /** reverse func. */
  public void reverseFunc() {
    // Implemented by subclass
  }

  @Override
  public void addChildOperator(QueryFilter op) {
    logger.error("cannot add child to leaf FilterOperator, now it's FunctionOperator");
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    FilterTypes.Function.serialize(byteBuffer);
    super.serializeWithoutType(byteBuffer);
  }

  public static FunctionFilter deserialize(ByteBuffer byteBuffer) {
    int filterTypeIndex = ReadWriteIOUtils.readInt(byteBuffer);
    int childSize = ReadWriteIOUtils.readInt(byteBuffer);
    List<QueryFilter> queryFilters = new ArrayList<>();
    for (int i = 0; i < childSize; i++) {
      queryFilters.add(FilterDeserializeUtil.deserialize(byteBuffer));
    }
    boolean isLeaf = ReadWriteIOUtils.readBool(byteBuffer);
    boolean isSingle = ReadWriteIOUtils.readBool(byteBuffer);
    PartialPath singlePath = null;
    if (isSingle) {
      singlePath = (PartialPath) PathDeserializeUtil.deserialize(byteBuffer);
    }
    int pathSetSize = ReadWriteIOUtils.readInt(byteBuffer);
    Set<PartialPath> pathSet = new HashSet<>();
    for (int i = 0; i < pathSetSize; i++) {
      pathSet.add((PartialPath) PathDeserializeUtil.deserialize(byteBuffer));
    }

    FunctionFilter queryFilter = new FunctionFilter(FilterType.values()[filterTypeIndex]);
    queryFilter.setChildren(queryFilters);
    queryFilter.setPathSet(pathSet);
    queryFilter.setSinglePath(singlePath);
    queryFilter.isLeaf = isLeaf;
    queryFilter.isSingle = isSingle;
    return queryFilter;
  }
}
