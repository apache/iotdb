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

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.MetaUtils;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.db.query.aggregation.impl.AvgAggrResult;
import org.apache.iotdb.db.query.factory.AggregateResultFactory;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AggregateUtils {
  /**
   * Transform an originalPath to a partial path that satisfies given level. Path nodes don't
   * satisfy the given level will be replaced by "*" except the sensor level, e.g.
   * generatePartialPathByLevel("root.sg.dh.d1.s1", 2) will return "root.*.dh.*.s1".
   *
   * <p>Especially, if count(*), then the sensor level will be replaced by "*" too.
   *
   * @return result partial path
   */
  public static String generatePartialPathByLevel(
      String originalPath, int[] pathLevels, boolean isCountStar) throws IllegalPathException {
    String[] nodes = MetaUtils.splitPathToDetachedPath(originalPath);
    Set<Integer> levelSet = new HashSet<>();
    for (int level : pathLevels) {
      levelSet.add(level);
    }

    StringBuilder transformedPath = new StringBuilder();
    transformedPath.append(nodes[0]).append(TsFileConstant.PATH_SEPARATOR);
    for (int k = 1; k < nodes.length - 1; k++) {
      if (levelSet.contains(k)) {
        transformedPath.append(nodes[k]);
      } else {
        transformedPath.append(IoTDBConstant.PATH_WILDCARD);
      }
      transformedPath.append(TsFileConstant.PATH_SEPARATOR);
    }
    if (isCountStar) {
      transformedPath.append(IoTDBConstant.PATH_WILDCARD);
    } else {
      transformedPath.append(nodes[nodes.length - 1]);
    }
    return transformedPath.toString();
  }

  /**
   * merge the raw record by level, for example raw record [timestamp, root.sg1.d1.s0,
   * root.sg1.d1.s1, root.sg1.d2.s2], level=1 and newRecord data is [100, 1, 1, 1] return [100, 3]
   *
   * @param newRecord
   * @param finalPaths
   * @return
   */
  public static List<AggregateResult> mergeRecordByPath(
      AggregationPlan plan, RowRecord newRecord, Map<String, AggregateResult> finalPaths)
      throws QueryProcessException {
    if (newRecord.getFields().size() < finalPaths.size()) {
      return Collections.emptyList();
    }
    List<AggregateResult> aggregateResultList = new ArrayList<>();
    for (int i = 0; i < newRecord.getFields().size(); i++) {
      if (newRecord.getFields().get(i) == null) {
        aggregateResultList.add(
            AggregateResultFactory.getAggrResultByName(
                plan.getDeduplicatedAggregations().get(i), plan.getDeduplicatedDataTypes().get(i)));
      } else {
        TSDataType dataType = newRecord.getFields().get(i).getDataType();
        AggregateResult aggRet =
            AggregateResultFactory.getAggrResultByName(
                plan.getDeduplicatedAggregations().get(i), dataType);
        if (aggRet.getAggregationType().equals(AggregationType.AVG)) {
          ((AvgAggrResult) aggRet)
              .setAvgResult(dataType, newRecord.getFields().get(i).getDoubleV());
        } else {
          switch (dataType) {
            case TEXT:
              aggRet.setBinaryValue(newRecord.getFields().get(i).getBinaryV());
              break;
            case INT32:
              aggRet.setIntValue(newRecord.getFields().get(i).getIntV());
              break;
            case INT64:
              aggRet.setLongValue(newRecord.getFields().get(i).getLongV());
              break;
            case FLOAT:
              aggRet.setFloatValue(newRecord.getFields().get(i).getFloatV());
              break;
            case DOUBLE:
              aggRet.setDoubleValue(newRecord.getFields().get(i).getDoubleV());
              break;
            case BOOLEAN:
              aggRet.setBooleanValue(newRecord.getFields().get(i).getBoolV());
              break;
            default:
              throw new UnSupportedDataTypeException(dataType.toString());
          }
        }
        aggregateResultList.add(aggRet);
      }
    }
    return mergeRecordByPath(plan, aggregateResultList, finalPaths);
  }

  public static List<AggregateResult> mergeRecordByPath(
      AggregationPlan plan,
      List<AggregateResult> aggResults,
      Map<String, AggregateResult> finalPaths)
      throws QueryProcessException {
    if (aggResults.size() < finalPaths.size()) {
      return Collections.emptyList();
    }
    for (Map.Entry<String, AggregateResult> entry : finalPaths.entrySet()) {
      finalPaths.put(entry.getKey(), null);
    }

    List<AggregateResult> resultSet = new ArrayList<>();
    List<PartialPath> dupPaths = plan.getDeduplicatedPaths();
    for (int i = 0; i < aggResults.size(); i++) {
      if (aggResults.get(i) != null) {
        //          String transformedPath =
        //              generatePartialPathByLevel(
        //                  dupPaths.get(i).getFullPath(), plan.getLevels(), plan.isCountStar());
        //          String key = plan.getDeduplicatedAggregations().get(i) + "(" + transformedPath +
        // ")";
        String rawPath = plan.getDeduplicatedAggregations().get(i) + "(" + dupPaths.get(i).getFullPath() + ")";
        String key = plan.getGroupByLevelController().getGroupedPath(rawPath);
        AggregateResult tempAggResult = finalPaths.get(key);
        if (tempAggResult == null) {
          finalPaths.put(key, aggResults.get(i));
        } else {
          tempAggResult.merge(aggResults.get(i));
          finalPaths.put(key, tempAggResult);
        }
      }
    }

    for (Map.Entry<String, AggregateResult> entry : finalPaths.entrySet()) {
      resultSet.add(entry.getValue());
    }
    return resultSet;
  }
}
