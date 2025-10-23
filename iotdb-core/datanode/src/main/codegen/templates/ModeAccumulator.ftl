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
<@pp.dropOutputFile />

<#list allDataTypes.types as type>

    <#assign className = "${type.dataType?cap_first}ModeAccumulator">
    <@pp.changeOutputFile name="/org/apache/iotdb/db/queryengine/execution/aggregation/${className}.java" />

package org.apache.iotdb.db.queryengine.execution.aggregation;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.external.commons.collections4.comparators.ComparatorChain;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

/*
* This class is generated using freemarker and the ${.template_name} template.
*/
@SuppressWarnings("unused")
public class ${className} implements Accumulator {
  // pair left records count of element, pair right records min time of element
  private final Map<${type.javaBoxName}, Pair<Long, Long>> countMap = new HashMap<>();

  <#if type.dataType != "boolean">
  private final int MAP_SIZE_THRESHOLD = IoTDBDescriptor.getInstance().getConfig().getModeMapSizeThreshold();

  </#if>
  @Override
  public void addInput(Column[] column, BitMap bitMap) {
    int count = column[0].getPositionCount();
    for (int i = 0; i < count; i++) {
      if (bitMap != null && !bitMap.isMarked(i)) {
        continue;
      }
      if (!column[1].isNull(i)) {
          final long time = column[0].getLong(i);
        countMap.compute(
            column[1].get${type.dataType?cap_first}(i),
            (k, v) ->
                v == null ? new Pair<>(1L, time) : new Pair<>(v.left + 1, Math.min(v.right, time)));
        <#if type.dataType != "boolean">

        if (countMap.size() > MAP_SIZE_THRESHOLD) {
          throw new RuntimeException(
              String.format(
                  "distinct values has exceeded the threshold %s when calculate Mode",
                  MAP_SIZE_THRESHOLD));
        }
        </#if>
      }
    }
  }

  @Override
  public void addIntermediate(Column[] partialResult) {
    checkArgument(partialResult.length == 1, "partialResult of Mode should be 1");
    checkArgument(!partialResult[0].isNull(0), "partialResult of Mode should not be null");
    deserializeAndMergeCountMap(partialResult[0].getBinary(0));
  }

  @Override
  public void addStatistics(Statistics statistics) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public void setFinal(Column finalResult) {
    if (finalResult.isNull(0)) {
      return;
    }

    // Step of ModeAccumulator is STATIC,
    // countMap only need to record one entry which key is finalResult
    countMap.put(finalResult.get${type.dataType?cap_first}(0), new Pair<>(0L, Long.MIN_VALUE));
  }

  @Override
  public void outputIntermediate(ColumnBuilder[] tsBlockBuilder) {
    tsBlockBuilder[0].writeBinary(serializeCountMap());
  }

  @Override
  public void outputFinal(ColumnBuilder tsBlockBuilder) {
    if (countMap.isEmpty()) {
      tsBlockBuilder.appendNull();
    } else {
      tsBlockBuilder.write${type.dataType?cap_first}(
          Collections.max(
                  countMap.entrySet(),
                  Map.Entry.comparingByValue(
                      new ComparatorChain<>(
                          ImmutableList.of(
                              (v1, v2) -> v1.left.compareTo(v2.left),
                              (v1, v2) -> v2.right.compareTo(v1.right)))))
              .getKey());
    }
  }

  @Override
  public void reset() {
    countMap.clear();
  }

  @Override
  public boolean hasFinalResult() {
    return false;
  }

  @Override
  public TSDataType[] getIntermediateType() {
    return new TSDataType[] {TSDataType.TEXT};
  }

  @Override
  public TSDataType getFinalType() {
    return ${type.tsDataType};
  }

  private Binary serializeCountMap() {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    try {
      ReadWriteIOUtils.write(countMap.size(), stream);
      for (Map.Entry<${type.javaBoxName}, Pair<Long, Long>> entry : countMap.entrySet()) {
        ReadWriteIOUtils.write(entry.getKey(), stream);
        ReadWriteIOUtils.write(entry.getValue().left, stream);
        ReadWriteIOUtils.write(entry.getValue().right, stream);
      }
    } catch (IOException e) {
      // Totally memory operation. This case won't happen.
    }
    return new Binary(stream.toByteArray());
  }

  private void deserializeAndMergeCountMap(Binary partialResult) {
    InputStream stream = new ByteArrayInputStream(partialResult.getValues());
    try {
      int size = ReadWriteIOUtils.readInt(stream);
      for (int i = 0; i < size; i++) {
        countMap.compute(
            ReadWriteIOUtils.read${type.dataType?cap_first}(stream),
            (k, v) -> {
              try {
                return v == null
                    ? new Pair<>(
                        ReadWriteIOUtils.readLong(stream), ReadWriteIOUtils.readLong(stream))
                    : new Pair<>(
                        v.left + ReadWriteIOUtils.readLong(stream),
                        Math.min(v.right, ReadWriteIOUtils.readLong(stream)));
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });
        <#if type.dataType != "boolean">

        if (countMap.size() > MAP_SIZE_THRESHOLD) {
          throw new RuntimeException(
              String.format(
                  "distinct values has exceeded the threshold %s when calculate Mode",
                  MAP_SIZE_THRESHOLD));
        }
        </#if>
      }
    } catch (IOException e) {
      // Totally memory operation. This case won't happen.
    }
  }
}

</#list>