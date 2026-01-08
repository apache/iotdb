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
import org.apache.iotdb.db.queryengine.execution.fragment.DataNodeQueryContext;
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
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.Utils.serializeTimeValue;
import static org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TableDeviceLastCache.EMPTY_PRIMITIVE_TYPE;
import static org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TableDeviceLastCache.EMPTY_TIME_VALUE_PAIR;
import static org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager.getTSDataType;

/**
 * This class is used to execute aggregation table scan when apply {@code canUseLastCacheOptimize()}
 */
public class LastQueryAggTableScanOperator extends AbstractAggTableScanOperator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(LastQueryAggTableScanOperator.class);

  private static final TableDeviceSchemaCache TABLE_DEVICE_SCHEMA_CACHE =
      TableDeviceSchemaCache.getInstance();

  private final QualifiedObjectName tableCompleteName;
  private final String dbName;
  private int outputDeviceIndex;
  private DeviceEntry currentDeviceEntry;
  private final List<DeviceEntry> cachedDeviceEntries;
  private final int allDeviceCount;

  private final boolean needUpdateCache;
  private final boolean needUpdateNullEntry;
  private final List<Integer> hitCachesIndexes;
  private final List<Pair<OptionalLong, TsPrimitiveType[]>> lastRowCacheResults;
  private final List<TimeValuePair[]> lastValuesCacheResults;
  private int currentHitCacheIndex = 0;

  // indicates the index of last(time) aggregation
  private int lastTimeAggregationIdx = -1;

  private final Map<DeviceEntry, Integer> deviceCountMap;
  private final DataNodeQueryContext dataNodeQueryContext;

  public LastQueryAggTableScanOperator(
      AbstractAggTableScanOperatorParameter parameter,
      List<DeviceEntry> cachedDeviceEntries,
      QualifiedObjectName qualifiedObjectName,
      List<Integer> hitCachesIndexes,
      List<Pair<OptionalLong, TsPrimitiveType[]>> lastRowCacheResults,
      List<TimeValuePair[]> lastValuesCacheResults,
      Map<DeviceEntry, Integer> deviceCountMap,
      DataNodeQueryContext dataNodeQueryContext) {

    super(parameter);

    // notice that: deviceEntries store all unCachedDeviceEntries
    this.allDeviceCount = cachedDeviceEntries.size() + parameter.deviceCount;
    this.cachedDeviceEntries = cachedDeviceEntries;
    this.needUpdateCache =
        LastQueryUtil.needUpdateCache(parameter.seriesScanOptions.getGlobalTimeFilter());
    this.needUpdateNullEntry =
        LastQueryUtil.needUpdateNullEntry(parameter.seriesScanOptions.getGlobalTimeFilter());
    this.hitCachesIndexes = hitCachesIndexes;
    this.lastRowCacheResults = lastRowCacheResults;
    this.lastValuesCacheResults = lastValuesCacheResults;
    this.tableCompleteName = qualifiedObjectName;
    this.dbName = qualifiedObjectName.getDatabaseName();

    this.operatorContext.recordSpecifiedInfo(
        PlanGraphPrinter.CACHED_DEVICE_NUMBER, Integer.toString(cachedDeviceEntries.size()));
    for (int i = 0; i < parameter.tableAggregators.size(); i++) {
      if (parameter.tableAggregators.get(i).getAccumulator() instanceof LastAccumulator) {
        lastTimeAggregationIdx = i;
      }
    }
    this.deviceCountMap = deviceCountMap;
    this.dataNodeQueryContext = dataNodeQueryContext;
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
      if (lastRowCacheResults != null) {
        buildResultUseLastRowCache();
      } else {
        checkState(lastValuesCacheResults != null, "lastValuesCacheResults shouldn't be null here");
        buildResultUseLastValuesCache();
      }
      return;
    }

    Optional<Boolean> b = calculateAggregationResultForCurrentTimeRange();
    if (b.isPresent() && b.get()) {
      timeIterator.resetCurTimeRange();
    }
  }

  private void buildResultUseLastRowCache() {
    appendGroupKeysToResult(cachedDeviceEntries, currentHitCacheIndex);
    Pair<OptionalLong, TsPrimitiveType[]> currentHitResult =
        lastRowCacheResults.get(currentHitCacheIndex);
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
              lastRowCacheResults.get(currentHitCacheIndex).getRight()[measurementIdx];
          long lastByTime = lastRowCacheResults.get(currentHitCacheIndex).getLeft().getAsLong();
          if (tsPrimitiveType == EMPTY_PRIMITIVE_TYPE) {
            // there is no data for this time series
            if (aggregator.getStep().isOutputPartial()) {
              columnBuilder.writeBinary(
                  new Binary(
                      serializeTimeValue(getTSDataType(schema.getType()), lastByTime, true, null)));
            } else {
              columnBuilder.appendNull();
            }
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

  private void buildResultUseLastValuesCache() {
    TimeValuePair[] currentHitResult = lastValuesCacheResults.get(currentHitCacheIndex);
    // if it is EMPTY_PRIMITIVE_TYPE, means there is no data in device
    TsPrimitiveType timeLastValue = currentHitResult[currentHitResult.length - 1].getValue();
    // when there is no data, no need to append result if the query is GROUP BY or output of
    // aggregator is partial (final operator will produce NULL result)
    if (timeLastValue == EMPTY_PRIMITIVE_TYPE
        && (groupingKeySize != 0 || tableAggregators.get(0).getStep().isOutputPartial())) {
      outputDeviceIndex++;
      currentHitCacheIndex++;
      return;
    }

    // there is no problem when the cache result doesn't contain time column, because if in such
    // case we will not use lastRowTime in later process
    long lastRowTime = currentHitResult[currentHitResult.length - 1].getTimestamp();
    appendGroupKeysToResult(cachedDeviceEntries, currentHitCacheIndex);
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
          if (aggregator.getAccumulator() instanceof LastDescAccumulator) {
            if (timeLastValue == EMPTY_PRIMITIVE_TYPE || id == null) {
              columnBuilder.appendNull();
            } else {
              if (aggregator.getStep().isOutputPartial()) {
                columnBuilder.writeBinary(
                    new Binary(
                        serializeTimeValue(
                            getTSDataType(schema.getType()),
                            lastRowTime,
                            new TsPrimitiveType.TsBinary(
                                new Binary(id, TSFileConfig.STRING_CHARSET)))));
              } else {
                columnBuilder.writeBinary(new Binary(id, TSFileConfig.STRING_CHARSET));
              }
            }
          } else {
            // last_by
            int measurementIdx = aggColumnsIndexArray[aggregatorInputChannels.get(channel)];
            long lastTime =
                lastValuesCacheResults.get(currentHitCacheIndex)[measurementIdx].getTimestamp();

            if (timeLastValue == EMPTY_PRIMITIVE_TYPE || id == null) {
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
          }
          break;
        case ATTRIBUTE:
          Binary attribute =
              cachedDeviceEntries.get(currentHitCacheIndex)
                  .getAttributeColumnValues()[aggColumnsIndexArray[columnIdx]];
          if (aggregator.getAccumulator() instanceof LastDescAccumulator) {
            if (timeLastValue == EMPTY_PRIMITIVE_TYPE || attribute == null) {
              columnBuilder.appendNull();
            } else {
              if (aggregator.getStep().isOutputPartial()) {
                columnBuilder.writeBinary(
                    new Binary(
                        serializeTimeValue(
                            getTSDataType(schema.getType()),
                            lastRowTime,
                            new TsPrimitiveType.TsBinary(attribute))));
              } else {
                columnBuilder.writeBinary(attribute);
              }
            }
          } else {
            int measurementIdx = aggColumnsIndexArray[aggregatorInputChannels.get(channel)];
            long lastTime =
                lastValuesCacheResults.get(currentHitCacheIndex)[measurementIdx].getTimestamp();

            // last_by
            if (timeLastValue == EMPTY_PRIMITIVE_TYPE || attribute == null) {
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
          }
          break;
        case TIME:
          if (aggregator.getAccumulator() instanceof LastDescAccumulator) {
            // for last(time) aggregation
            if (timeLastValue == EMPTY_PRIMITIVE_TYPE) {
              columnBuilder.appendNull();
            } else {
              if (aggregator.getStep().isOutputPartial()) {
                columnBuilder.writeBinary(
                    new Binary(
                        serializeTimeValue(
                            getTSDataType(schema.getType()),
                            lastRowTime,
                            new TsPrimitiveType.TsLong(lastRowTime))));
              } else {
                columnBuilder.writeTsPrimitiveType(new TsPrimitiveType.TsLong(lastRowTime));
              }
            }
          } else {
            // for aggregation like last_by(time,s1)
            int measurementIdx = aggColumnsIndexArray[aggregatorInputChannels.get(channel + 1)];
            long lastTime =
                lastValuesCacheResults.get(currentHitCacheIndex)[measurementIdx].getTimestamp();
            TsPrimitiveType tsPrimitiveType =
                lastValuesCacheResults.get(currentHitCacheIndex)[measurementIdx].getValue();

            if (tsPrimitiveType == EMPTY_PRIMITIVE_TYPE) {
              // there is no data
              if (aggregator.getStep().isOutputPartial()) {
                columnBuilder.writeBinary(
                    new Binary(
                        serializeTimeValue(getTSDataType(schema.getType()), lastTime, true, null)));
              } else {
                columnBuilder.appendNull();
              }
            } else {
              if (aggregator.getStep().isOutputPartial()) {
                // output: xDataType, yLastTime, xIsNull, xResult
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
          }
          break;
        case FIELD:
          checkState(
              aggregator.getAccumulator() instanceof LastDescAccumulator,
              "Accumulator should be LastDescAccumulator when reach here");

          int measurementIdx = aggColumnsIndexArray[aggregatorInputChannels.get(channel)];
          long lastTime =
              lastValuesCacheResults.get(currentHitCacheIndex)[measurementIdx].getTimestamp();
          TsPrimitiveType tsPrimitiveType =
              lastValuesCacheResults.get(currentHitCacheIndex)[measurementIdx].getValue();

          if (tsPrimitiveType == EMPTY_PRIMITIVE_TYPE) {
            // there is no data for this time series
            columnBuilder.appendNull();
          } else {
            if (aggregator.getStep().isOutputPartial()) {
              columnBuilder.writeBinary(
                  new Binary(
                      serializeTimeValue(
                          getTSDataType(schema.getType()), lastTime, tsPrimitiveType)));
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

  private void updateLastCacheUseLastRowIfPossible() {
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
            updateMeasurementList.add("");
            if (i == lastTimeAggregationIdx) {
              LastDescAccumulator lastAccumulator =
                  (LastDescAccumulator) tableAggregator.getAccumulator();
              if (lastAccumulator.hasInitResult()) {
                updateTimeValuePairList.add(
                    new TimeValuePair(
                        lastAccumulator.getMaxTime(),
                        new TsPrimitiveType.TsLong(lastAccumulator.getMaxTime())));
              } else {
                currentDeviceEntry = deviceEntries.get(currentDeviceIndex);
                updateTimeValuePairList.add(EMPTY_TIME_VALUE_PAIR);
              }
            } else {
              LastByDescAccumulator lastByAccumulator =
                  (LastByDescAccumulator) tableAggregator.getAccumulator();
              if (lastByAccumulator.hasInitResult() && !lastByAccumulator.isXNull()) {
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
          updateMeasurementList.add(schema.getName());
          if (lastByAccumulator.hasInitResult() && !lastByAccumulator.isXNull()) {
            long lastByTime = lastByAccumulator.getLastTimeOfY();

            if (!hasSetLastTime) {
              hasSetLastTime = true;
              updateMeasurementList.add("");
              updateTimeValuePairList.add(
                  new TimeValuePair(lastByTime, new TsPrimitiveType.TsLong(lastByTime)));
            }

            updateTimeValuePairList.add(
                new TimeValuePair(
                    lastByTime, cloneTsPrimitiveType(lastByAccumulator.getXResult())));
          } else {
            updateTimeValuePairList.add(EMPTY_TIME_VALUE_PAIR);
          }
          break;
        default:
          break;
      }

      channel += tableAggregator.getChannelCount();
    }

    checkIfUpdated(updateMeasurementList, updateTimeValuePairList);
  }

  private void updateLastCacheUseLastValuesIfPossible() {
    int channel = 0;
    List<String> updateMeasurementList = new ArrayList<>();
    List<TimeValuePair> updateTimeValuePairList = new ArrayList<>();
    boolean hasSetLastTime = false;
    for (TableAggregator tableAggregator : tableAggregators) {
      ColumnSchema schema = aggColumnSchemas.get(aggregatorInputChannels.get(channel));

      switch (schema.getColumnCategory()) {
        case TIME:
        case TAG:
        case ATTRIBUTE:
          if (!hasSetLastTime && tableAggregator.getAccumulator() instanceof LastDescAccumulator) {
            hasSetLastTime = true;
            LastDescAccumulator lastAccumulator =
                (LastDescAccumulator) tableAggregator.getAccumulator();
            updateMeasurementList.add("");
            if (lastAccumulator.hasInitResult()) {
              updateTimeValuePairList.add(
                  new TimeValuePair(
                      lastAccumulator.getMaxTime(),
                      new TsPrimitiveType.TsLong(lastAccumulator.getMaxTime())));
            } else {
              currentDeviceEntry = deviceEntries.get(currentDeviceIndex);
              updateTimeValuePairList.add(EMPTY_TIME_VALUE_PAIR);
            }
          }
          break;
        case FIELD:
          checkState(
              tableAggregator.getAccumulator() instanceof LastDescAccumulator,
              "Accumulator should be LastDescAccumulator when reach here");
          LastDescAccumulator lastAccumulator =
              (LastDescAccumulator) tableAggregator.getAccumulator();
          updateMeasurementList.add(schema.getName());
          if (lastAccumulator.hasInitResult()) {
            updateTimeValuePairList.add(
                new TimeValuePair(
                    lastAccumulator.getMaxTime(),
                    cloneTsPrimitiveType(lastAccumulator.getLastValue())));
          } else {
            updateTimeValuePairList.add(EMPTY_TIME_VALUE_PAIR);
          }
          break;
        default:
          break;
      }

      channel += tableAggregator.getChannelCount();
    }

    checkIfUpdated(updateMeasurementList, updateTimeValuePairList);
  }

  private void checkIfUpdated(
      List<String> updateMeasurementList, List<TimeValuePair> updateTimeValuePairList) {
    if (!updateMeasurementList.isEmpty()) {
      currentDeviceEntry = deviceEntries.get(currentDeviceIndex);

      boolean deviceInMultiRegion =
          deviceCountMap != null && deviceCountMap.containsKey(currentDeviceEntry);
      if (!deviceInMultiRegion) {
        TABLE_DEVICE_SCHEMA_CACHE.updateLastCacheIfExists(
            dbName,
            currentDeviceEntry.getDeviceID(),
            updateMeasurementList.toArray(new String[0]),
            updateTimeValuePairList.toArray(new TimeValuePair[0]));
        return;
      }

      dataNodeQueryContext.lock(true);
      try {
        Pair<Integer, Map<String, TimeValuePair>> deviceInfo =
            dataNodeQueryContext.getDeviceInfo(tableCompleteName, currentDeviceEntry);
        Map<String, TimeValuePair> values = deviceInfo.getRight();

        int size = updateMeasurementList.size();
        for (int i = 0; i < size; i++) {
          String measurementName = updateMeasurementList.get(i);
          TimeValuePair timeValuePair = updateTimeValuePairList.get(i);
          if (values.containsKey(measurementName)) {
            TimeValuePair oldValue = values.get(measurementName);
            if (timeValuePair.getTimestamp() > oldValue.getTimestamp()) {
              values.put(measurementName, timeValuePair);
            }
          } else {
            values.put(measurementName, timeValuePair);
          }
        }

        deviceInfo.left--;
        if (deviceInfo.left == 0) {
          dataNodeQueryContext.updateLastCache(tableCompleteName, currentDeviceEntry);
        }
      } finally {
        dataNodeQueryContext.unLock(true);
      }
    }
  }

  @Override
  protected void updateResultTsBlock() {
    appendAggregationResult();

    if (needUpdateCache && timeIterator.hasCachedTimeRange()) {
      if (lastRowCacheResults != null) {
        updateLastCacheUseLastRowIfPossible();
      } else {
        checkState(lastValuesCacheResults != null, "lastValuesCacheResults shouldn't be null here");
        updateLastCacheUseLastValuesIfPossible();
      }
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
      case OBJECT:
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
        + RamUsageEstimator.sizeOfCollection(lastRowCacheResults);
  }
}
