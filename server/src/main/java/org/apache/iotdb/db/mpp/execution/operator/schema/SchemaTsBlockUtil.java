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

package org.apache.iotdb.db.mpp.execution.operator.schema;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiConsumer;

import static org.apache.iotdb.tsfile.read.common.block.TsBlockBuilderStatus.DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;

public class SchemaTsBlockUtil {

  private static final long MAX_SIZE = DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;

  public static <T> List<TsBlock> transferSchemaResultToTsBlockList(
      Iterator<T> schemaRegionResultIterator,
      List<TSDataType> outputDataTypes,
      BiConsumer<T, TsBlockBuilder> consumer) {
    List<TsBlock> result = new ArrayList<>();
    TsBlockBuilder tsBlockBuilder = new TsBlockBuilder(outputDataTypes);
    T schemaRegionResultElement;
    while (schemaRegionResultIterator.hasNext()) {
      schemaRegionResultElement = schemaRegionResultIterator.next();
      consumer.accept(schemaRegionResultElement, tsBlockBuilder);
      if (tsBlockBuilder.getRetainedSizeInBytes() >= MAX_SIZE) {
        result.add(tsBlockBuilder.build());
        tsBlockBuilder = new TsBlockBuilder(outputDataTypes);
      }
    }
    if (!tsBlockBuilder.isEmpty()) {
      result.add(tsBlockBuilder.build());
    }
    return result;
  }
}
