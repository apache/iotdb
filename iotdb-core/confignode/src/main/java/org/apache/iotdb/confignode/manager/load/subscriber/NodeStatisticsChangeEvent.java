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

package org.apache.iotdb.confignode.manager.load.subscriber;

import org.apache.iotdb.confignode.manager.load.cache.node.NodeStatistics;

import org.apache.tsfile.read.common.type.BinaryType;
import org.apache.tsfile.read.common.type.BooleanType;
import org.apache.tsfile.read.common.type.DoubleType;
import org.apache.tsfile.read.common.type.FloatType;
import org.apache.tsfile.read.common.type.IntType;
import org.apache.tsfile.read.common.type.LongType;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.TypeEnum;
import org.apache.tsfile.read.common.type.UnknownType;
import org.apache.tsfile.utils.Pair;

import java.util.Map;

/** NodeStatisticsChangeEvent represents the change of Node statistics. */
public class NodeStatisticsChangeEvent {

  // Map<NodeId, Pair<old NodeStatistics, new NodeStatistics>>
  private final Map<Integer, Pair<NodeStatistics, NodeStatistics>> differentNodeStatisticsMap;

  public NodeStatisticsChangeEvent(
      Map<Integer, Pair<NodeStatistics, NodeStatistics>> differentNodeStatisticsMap) {
    this.differentNodeStatisticsMap = differentNodeStatisticsMap;
  }

  public Map<Integer, Pair<NodeStatistics, NodeStatistics>> getDifferentNodeStatisticsMap() {
    return differentNodeStatisticsMap;
  }

  public static Type getType(TypeEnum typeEnum) {
    switch (typeEnum) {
      case INT32:
        return IntType.getInstance();
      case INT64:
        return LongType.getInstance();
      case FLOAT:
        return FloatType.getInstance();
      case DOUBLE:
        return DoubleType.getInstance();
      case BOOLEAN:
        return BooleanType.getInstance();
      case TEXT:
        return BinaryType.getInstance();
      case UNKNOWN:
        return UnknownType.getInstance();
      default:
        throw new UnsupportedOperationException(
            String.format("Invalid TypeEnum for TypeFactory: %s", typeEnum));
    }
  }
}
