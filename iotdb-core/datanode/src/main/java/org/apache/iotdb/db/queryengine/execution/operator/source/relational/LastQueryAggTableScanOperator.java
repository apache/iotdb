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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational;

import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.operator.process.last.LastQueryUtil;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.LastAccumulator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.LastByDescAccumulator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.LastDescAccumulator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.TableAggregator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanGraphPrinter;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TableDeviceSchemaCache;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.Utils.serializeTimeValue;
import static org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TableDeviceLastCache.EMPTY_PRIMITIVE_TYPE;
import static org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager.getTSDataType;

/**
 * This class is used to execute aggregation table scan when apply {@code canUseLastCacheOptimize()}
 */
public class LastQueryAggTableScanOperator extends AbstractAggTableScanOperator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(LastQueryAggTableScanOperator.class);

  private static final TableDeviceSchemaCache TABLE_DEVICE_SCHEMA_CACHE =
      TableDeviceSchemaCache.getInstance();

  private final String dbName;
  private int outputDeviceIndex;
  private DeviceEntry currentDeviceEntry;
  private final List<DeviceEntry> cachedDeviceEntries;
  private final int allDeviceCount;

  private final boolean needUpdateCache;
  private final boolean needUpdateNullEntry;
  private final List<Integer> hitCachesIndexes;
  private final List<Pair<OptionalLong, TsPrimitiveType[]>> hitCachedResults;
  private int currentHitCacheIndex = 0;

  // indicates the index of last(time) aggregation
  private int lastTimeAggregationIdx = -1;

  public LastQueryAggTableScanOperator(
      AbstractAggTableScanOperatorParameter parameter,
      List<DeviceEntry> cachedDeviceEntries,
      QualifiedObjectName qualifiedObjectName,
      List<Integer> hitCachesIndexes,
      List<Pair<OptionalLong, TsPrimitiveType[]>> hitCachedResults) {

    super(parameter);

    // notice that: deviceEntries store all unCachedDeviceEntries
    this.allDeviceCount = cachedDeviceEntries.size() + parameter.deviceCount;
    this.cachedDeviceEntries = cachedDeviceEntries;
    this.needUpdateCache =
        LastQueryUtil.needUpdateCache(parameter.seriesScanOptions.getGlobalTimeFilter());
    this.needUpdateNullEntry =
        LastQueryUtil.needUpdateNullEntry(parameter.seriesScanOptions.getGlobalTimeFilter());
    this.hitCachesIndexes = hitCachesIndexes;
    this.hitCachedResults = hitCachedResults;
    this.dbName = qualifiedObjectName.getDatabaseName();

    this.operatorContext.recordSpecifiedInfo(
        PlanGraphPrinter.CACHED_DEVICE_NUMBER, Integer.toString(cachedDeviceEntries.size()));
    for (int i = 0; i < parameter.tableAggregators.size(); i++) {
      if (parameter.tableAggregators.get(i).getAccumulator() instanceof LastAccumulator) {
        lastTimeAggregationIdx = i;
      }
    }
  }

  @Override
  public boolean hasNext() throws Exception {
    if (retainedTsBlock != null) {
      return true;
    }

    return outputDeviceIndex < allDeviceCount;
  }

  @Override
  public TsBlock next() throws Exception {
    long maxRuntime = operatorContext.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);
    long start = System.nanoTime();

    if (retainedTsBlock != null) {
      return getResultFromRetainedTsBlock();
    }

    while (System.nanoTime() - start < maxRuntime
        && !resultTsBlockBuilder.isFull()
        && outputDeviceIndex < allDeviceCount) {
      processCurrentDevice();
    }

    if (resultTsBlockBuilder.isEmpty()) {
      return null;
    }

    buildResultTsBlock();
    return checkTsBlockSizeAndGetResult();
  }

  /** Main process logic, calc the last aggregation results of current device. */
  private void processCurrentDevice() {
    if (currentHitCacheIndex < hitCachesIndexes.size()
        && outputDeviceIndex == hitCachesIndexes.get(currentHitCacheIndex)) {
      currentDeviceEntry = cachedDeviceEntries.get(currentHitCacheIndex);
      buildResultUseLastCache();
      return;
    }

    if (calculateAggregationResultForCurrentTimeRange()) {
      timeIterator.resetCurTimeRange();
    }
  }

  private void buildResultUseLastCache() {
    appendGroupKeysToResult(cachedDeviceEntries, currentHitCacheIndex);
    Pair<OptionalLong, TsPrimitiveType[]> currentHitResult =
        hitCachedResults.get(currentHitCacheIndex);
    long lastTime = currentHitResult.getLeft().getAsLong();
    int channel = 0;
    for (int i = 0; i < tableAggregators.size(); i++) {
      TableAggregator aggregator = tableAggregators.get(i);
      ColumnBuilder columnBuilder = resultTsBlockBuilder.getColumnBuilder(groupingKeySize + i);
      int columnIdx = aggregatorInputChannels.get(channel);
      ColumnSchema schema = aggColumnSchemas.get(columnIdx);
      TsTableColumnCategory category = schema.getColumnCategory();
      switch (category) {
        case TAG:
          String id =
              getNthIdColumnValue(
                  cachedDeviceEntries.get(currentHitCacheIndex), aggColumnsIndexArray[columnIdx]);
          if (id == null) {
            if (aggregator.getStep().isOutputPartial()) {
              columnBuilder.writeBinary(
                  new Binary(
                      serializeTimeValue(getTSDataType(schema.getType()), lastTime, true, null)));
            } else {
              columnBuilder.appendNull();
            }
          } else {
            if (aggregator.getStep().isOutputPartial()) {
              columnBuilder.writeBinary(
                  new Binary(
                      serializeTimeValue(
                          getTSDataType(schema.getType()),
                          lastTime,
                          false,
                          new TsPrimitiveType.TsBinary(
                              new Binary(id, TSFileConfig.STRING_CHARSET)))));
            } else {
              columnBuilder.writeBinary(new Binary(id, TSFileConfig.STRING_CHARSET));
            }
          }
          break;
        case ATTRIBUTE:
          Binary attribute =
              cachedDeviceEntries.get(currentHitCacheIndex)
                  .getAttributeColumnValues()[aggColumnsIndexArray[columnIdx]];
          if (attribute == null) {
            if (aggregator.getStep().isOutputPartial()) {
              columnBuilder.writeBinary(
                  new Binary(
                      serializeTimeValue(getTSDataType(schema.getType()), lastTime, true, null)));
            } else {
              columnBuilder.appendNull();
            }
          } else {
            if (aggregator.getStep().isOutputPartial()) {
              columnBuilder.writeBinary(
                  new Binary(
                      serializeTimeValue(
                          getTSDataType(schema.getType()),
                          lastTime,
                          false,
                          new TsPrimitiveType.TsBinary(attribute))));
            } else {
              columnBuilder.writeBinary(attribute);
            }
          }
          break;
        case TIME:
          if (aggregator.getAccumulator() instanceof LastDescAccumulator) {
            // for last(time) aggregation
            if (aggregator.getStep().isOutputPartial()) {
              columnBuilder.writeBinary(
                  new Binary(
                      serializeTimeValue(
                          getTSDataType(schema.getType()),
                          lastTime,
                          new TsPrimitiveType.TsLong(lastTime))));
            } else {
              columnBuilder.writeTsPrimitiveType(new TsPrimitiveType.TsLong(lastTime));
            }
          } else {
            // for last_by(time,time) aggregation
            if (aggregator.getStep().isOutputPartial()) {
              columnBuilder.writeBinary(
                  new Binary(
                      serializeTimeValue(
                          getTSDataType(schema.getType()),
                          lastTime,
                          false,
                          new TsPrimitiveType.TsLong(lastTime))));
            } else {
              columnBuilder.writeTsPrimitiveType(new TsPrimitiveType.TsLong(lastTime));
            }
          }
          break;
        case FIELD:
          int measurementIdx = aggColumnsIndexArray[aggregatorInputChannels.get(channel)];
          TsPrimitiveType tsPrimitiveType =
              hitCachedResults.get(currentHitCacheIndex).getRight()[measurementIdx];
          long lastByTime = hitCachedResults.get(currentHitCacheIndex).getLeft().getAsLong();
          if (tsPrimitiveType == EMPTY_PRIMITIVE_TYPE) {
            throw new IllegalStateException(
                "If the read value is [EMPTY_PRIMITIVE_TYPE], we should never reach here");
          } else {
            if (aggregator.getStep().isOutputPartial()) {
              columnBuilder.writeBinary(
                  new Binary(
                      serializeTimeValue(
                          getTSDataType(schema.getType()), lastByTime, false, tsPrimitiveType)));
            } else {
              columnBuilder.writeTsPrimitiveType(tsPrimitiveType);
            }
          }
          break;
        default:
          throw new IllegalStateException("Unsupported category: " + category);
      }

      channel += aggregator.getChannelCount();
    }

    resultTsBlockBuilder.declarePosition();
    outputDeviceIndex++;
    currentHitCacheIndex++;
  }

  private void updateLastCacheIfPossible() {
    if (!needUpdateCache) {
      return;
    }

    int channel = 0;
    List<String> updateMeasurementList = new ArrayList<>();
    List<TimeValuePair> updateTimeValuePairList = new ArrayList<>();
    boolean hasSetLastTime = false;
    for (int i = 0; i < tableAggregators.size(); i++) {
      TableAggregator tableAggregator = tableAggregators.get(i);
      ColumnSchema schema = aggColumnSchemas.get(aggregatorInputChannels.get(channel));

      switch (schema.getColumnCategory()) {
        case TIME:
          if (!hasSetLastTime) {
            hasSetLastTime = true;
            if (i == lastTimeAggregationIdx) {
              LastDescAccumulator lastAccumulator =
                  (LastDescAccumulator) tableAggregator.getAccumulator();
              if (lastAccumulator.hasInitResult()) {
                updateMeasurementList.add("");
                updateTimeValuePairList.add(
                    new TimeValuePair(
                        lastAccumulator.getMaxTime(),
                        new TsPrimitiveType.TsLong(lastAccumulator.getMaxTime())));
              }
            } else {
              LastByDescAccumulator lastByAccumulator =
                  (LastByDescAccumulator) tableAggregator.getAccumulator();
              if (lastByAccumulator.hasInitResult() && !lastByAccumulator.isXNull()) {
                updateMeasurementList.add("");
                updateTimeValuePairList.add(
                    new TimeValuePair(
                        lastByAccumulator.getLastTimeOfY(),
                        new TsPrimitiveType.TsLong(lastByAccumulator.getLastTimeOfY())));
              }
            }
          }
          break;
        case FIELD:
          LastByDescAccumulator lastByAccumulator =
              (LastByDescAccumulator) tableAggregator.getAccumulator();
          // only can update LastCache when last_by return non-null value
          if (lastByAccumulator.hasInitResult() && !lastByAccumulator.isXNull()) {
            long lastByTime = lastByAccumulator.getLastTimeOfY();

            if (!hasSetLastTime) {
              hasSetLastTime = true;
              updateMeasurementList.add("");
              updateTimeValuePairList.add(
                  new TimeValuePair(lastByTime, new TsPrimitiveType.TsLong(lastByTime)));
            }

            updateMeasurementList.add(schema.getName());
            updateTimeValuePairList.add(
                new TimeValuePair(
                    lastByTime, cloneTsPrimitiveType(lastByAccumulator.getXResult())));
          }
          break;
        default:
          break;
      }

      channel += tableAggregator.getChannelCount();
    }

    if (!updateMeasurementList.isEmpty()) {
      String[] updateMeasurementArray = updateMeasurementList.toArray(new String[0]);
      TimeValuePair[] updateTimeValuePairArray =
          updateTimeValuePairList.toArray(new TimeValuePair[0]);
      currentDeviceEntry = deviceEntries.get(currentDeviceIndex);
      TABLE_DEVICE_SCHEMA_CACHE.updateLastCacheIfExists(
          dbName,
          currentDeviceEntry.getDeviceID(),
          updateMeasurementArray,
          updateTimeValuePairArray);
    }
  }

  @Override
  protected void updateResultTsBlock() {
    appendAggregationResult();

    if (timeIterator.hasCachedTimeRange()) {
      updateLastCacheIfPossible();
    }

    outputDeviceIndex++;

    // after appendAggregationResult invoked, aggregators must be cleared
    resetTableAggregators();
  }

  @Override
  String getNthIdColumnValue(DeviceEntry deviceEntry, int idColumnIndex) {
    // +1 for skipping the table name segment
    return ((String) deviceEntry.getNthSegment(idColumnIndex + 1));
  }

  private TsPrimitiveType cloneTsPrimitiveType(TsPrimitiveType originalValue) {
    switch (originalValue.getDataType()) {
      case BOOLEAN:
        return new TsPrimitiveType.TsBoolean(originalValue.getBoolean());
      case INT32:
      case DATE:
        return new TsPrimitiveType.TsInt(originalValue.getInt());
      case INT64:
      case TIMESTAMP:
        return new TsPrimitiveType.TsLong(originalValue.getLong());
      case FLOAT:
        return new TsPrimitiveType.TsFloat(originalValue.getFloat());
      case DOUBLE:
        return new TsPrimitiveType.TsDouble(originalValue.getDouble());
      case TEXT:
      case BLOB:
      case STRING:
        return new TsPrimitiveType.TsBinary(originalValue.getBinary());
      case VECTOR:
        return new TsPrimitiveType.TsVector(originalValue.getVector());
      default:
        throw new UnSupportedDataTypeException(
            "Unsupported data type:" + originalValue.getDataType());
    }
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(seriesScanUtil)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(sourceId)
        + (resultTsBlockBuilder == null ? 0 : resultTsBlockBuilder.getRetainedSizeInBytes())
        + RamUsageEstimator.sizeOfCollection(deviceEntries)
        + RamUsageEstimator.sizeOfCollection(cachedDeviceEntries)
        + RamUsageEstimator.sizeOfCollection(hitCachedResults);
  }
}
