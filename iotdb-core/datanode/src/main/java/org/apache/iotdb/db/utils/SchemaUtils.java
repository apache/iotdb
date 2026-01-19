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

import org.apache.iotdb.common.rpc.thrift.TAggregationType;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.utils.constant.SqlConstant;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.AbstractAlignedChunkMetadata;
import org.apache.tsfile.file.metadata.AbstractAlignedTimeSeriesMetadata;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.IntColumn;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.utils.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.iotdb.db.queryengine.execution.operator.AggregationUtil.addPartialSuffix;

public class SchemaUtils {

  private static final Map<TSDataType, Class> dataTypeColumnClassMap;
  public static final Logger logger = LoggerFactory.getLogger(SchemaUtils.class);
  private static final Binary EMPTY_BINARY = new Binary("", StandardCharsets.UTF_8);

  static {
    dataTypeColumnClassMap = new HashMap<>();
    dataTypeColumnClassMap.put(TSDataType.INT32, IntColumn.class);
    dataTypeColumnClassMap.put(TSDataType.DATE, IntColumn.class);

    dataTypeColumnClassMap.put(TSDataType.INT64, LongColumn.class);
    dataTypeColumnClassMap.put(TSDataType.TIMESTAMP, LongColumn.class);

    dataTypeColumnClassMap.put(TSDataType.STRING, BinaryColumn.class);
    dataTypeColumnClassMap.put(TSDataType.BLOB, BinaryColumn.class);
    dataTypeColumnClassMap.put(TSDataType.TEXT, BinaryColumn.class);
  }

  private SchemaUtils() {}

  /**
   * If the datatype of 'aggregation' depends on 'measurementDataType' (min_value, max_value),
   * return 'measurementDataType' directly, or return a list whose elements are all the datatype of
   * 'aggregation' and its length is the same as 'measurementDataType'.
   *
   * @param measurementDataType raw measurement type
   * @param aggregation aggregation type
   * @return measurementDataType directly, or return a list whose elements are all the datatype of
   *     aggregation and its length is the same as 'measurementDataType'
   */
  public static List<TSDataType> getAggregatedDataTypes(
      List<TSDataType> measurementDataType, String aggregation) {
    TSDataType dataType = getBuiltinAggregationTypeByFuncName(aggregation);
    if (dataType != null) {
      return Collections.nCopies(measurementDataType.size(), dataType);
    }
    return measurementDataType;
  }

  public static TSDataType getSeriesTypeByPath(TSDataType seriesType, String aggregation) {
    TSDataType dataType = getBuiltinAggregationTypeByFuncName(aggregation);
    if (dataType != null) {
      return dataType;
    } else {
      return seriesType;
    }
  }

  /**
   * @param aggregation aggregation function
   * @return the data type of the aggregation or null if it aggregation is null
   */
  public static TSDataType getBuiltinAggregationTypeByFuncName(String aggregation) {
    if (aggregation == null) {
      return null;
    }
    switch (aggregation.toLowerCase()) {
      case SqlConstant.MIN_TIME:
      case SqlConstant.MAX_TIME:
      case SqlConstant.COUNT:
      case SqlConstant.TIME_DURATION:
      case SqlConstant.COUNT_TIME:
        return TSDataType.INT64;
      case SqlConstant.AVG:
      case SqlConstant.SUM:
      case SqlConstant.STDDEV:
      case SqlConstant.STDDEV_POP:
      case SqlConstant.STDDEV_SAMP:
      case SqlConstant.VARIANCE:
      case SqlConstant.VAR_POP:
      case SqlConstant.VAR_SAMP:
        return TSDataType.DOUBLE;
        // Partial aggregation names
      case SqlConstant.STDDEV + "_partial":
      case SqlConstant.STDDEV_POP + "_partial":
      case SqlConstant.STDDEV_SAMP + "_partial":
      case SqlConstant.VARIANCE + "_partial":
      case SqlConstant.VAR_POP + "_partial":
      case SqlConstant.VAR_SAMP + "_partial":
      case SqlConstant.MAX_BY + "_partial":
      case SqlConstant.MIN_BY + "_partial":
        return TSDataType.TEXT;
      case SqlConstant.LAST_VALUE:
      case SqlConstant.FIRST_VALUE:
      case SqlConstant.MIN_VALUE:
      case SqlConstant.MAX_VALUE:
      case SqlConstant.MODE:
      case SqlConstant.MAX_BY:
      case SqlConstant.MIN_BY:
      default:
        return null;
    }
  }

  /**
   * Return aggregation function name by given function type. If aggregation type is UDAF, you shall
   * acquire its name in other ways.
   *
   * @param aggregationType aggregation type
   * @return the name of the aggregation or null if its aggregation type is null
   */
  public static String getBuiltinAggregationName(TAggregationType aggregationType) {
    if (aggregationType == null) {
      return null;
    }
    switch (aggregationType) {
      case COUNT:
        return SqlConstant.COUNT;
      case AVG:
        return SqlConstant.AVG;
      case SUM:
        return SqlConstant.SUM;
      case FIRST_VALUE:
        return SqlConstant.FIRST_VALUE;
      case LAST_VALUE:
        return SqlConstant.LAST_VALUE;
      case MAX_TIME:
        return SqlConstant.MAX_TIME;
      case MIN_TIME:
        return SqlConstant.MIN_TIME;
      case MAX_VALUE:
        return SqlConstant.MAX_VALUE;
      case MIN_VALUE:
        return SqlConstant.MIN_VALUE;
      case EXTREME:
        return SqlConstant.EXTREME;
      case COUNT_IF:
        return SqlConstant.COUNT_IF;
      case TIME_DURATION:
        return SqlConstant.TIME_DURATION;
      case MODE:
        return SqlConstant.MODE;
      case COUNT_TIME:
        return SqlConstant.COUNT_TIME;
      case STDDEV:
        return SqlConstant.STDDEV;
      case STDDEV_POP:
        return SqlConstant.STDDEV_POP;
      case STDDEV_SAMP:
        return SqlConstant.STDDEV_SAMP;
      case VARIANCE:
        return SqlConstant.VARIANCE;
      case VAR_POP:
        return SqlConstant.VAR_POP;
      case VAR_SAMP:
        return SqlConstant.VAR_SAMP;
      default:
        return null;
    }
  }

  /**
   * judge whether the order of aggregation calculation is consistent with the order of traversing
   * data
   */
  public static boolean isConsistentWithScanOrder(
      TAggregationType aggregationFunction, Ordering scanOrder) {
    boolean ascending = scanOrder == Ordering.ASC;
    switch (aggregationFunction) {
      case MIN_TIME:
      case FIRST_VALUE:
        return ascending;
      case MAX_TIME:
      case LAST_VALUE:
        return !ascending;
      case SUM:
      case MIN_VALUE:
      case MAX_VALUE:
      case EXTREME:
      case COUNT:
      case COUNT_TIME:
      case AVG:
      case TIME_DURATION:
      case STDDEV:
      case STDDEV_POP:
      case STDDEV_SAMP:
      case VARIANCE:
      case VAR_POP:
      case VAR_SAMP:
      case MAX_BY:
      case MIN_BY:
      case UDAF:
        return true;
      default:
        throw new IllegalArgumentException(
            String.format("Invalid Aggregation function: %s", aggregationFunction));
    }
  }

  public static void checkDataTypeWithEncoding(TSDataType dataType, TSEncoding encoding)
      throws MetadataException {
    if (!encoding.isSupported(dataType)) {
      throw new MetadataException(
          String.format("encoding %s does not support %s", encoding, dataType), true);
    }
  }

  public static List<String> splitPartialBuiltinAggregation(TAggregationType aggregationType) {
    switch (aggregationType) {
      case FIRST_VALUE:
        return Collections.singletonList(SqlConstant.MIN_TIME);
      case LAST_VALUE:
        return Collections.singletonList(SqlConstant.MAX_TIME);
      case STDDEV:
        return Collections.singletonList(addPartialSuffix(SqlConstant.STDDEV));
      case STDDEV_POP:
        return Collections.singletonList(addPartialSuffix(SqlConstant.STDDEV_POP));
      case STDDEV_SAMP:
        return Collections.singletonList(addPartialSuffix(SqlConstant.STDDEV_SAMP));
      case VARIANCE:
        return Collections.singletonList(addPartialSuffix(SqlConstant.VARIANCE));
      case VAR_POP:
        return Collections.singletonList(addPartialSuffix(SqlConstant.VAR_POP));
      case VAR_SAMP:
        return Collections.singletonList(addPartialSuffix(SqlConstant.VAR_SAMP));
      case MAX_BY:
        return Collections.singletonList(addPartialSuffix(SqlConstant.MAX_BY));
      case MIN_BY:
        return Collections.singletonList(addPartialSuffix(SqlConstant.MIN_BY));
      case AVG:
        return Arrays.asList(SqlConstant.COUNT, SqlConstant.SUM);
      case TIME_DURATION:
        return Arrays.asList(SqlConstant.MAX_TIME, SqlConstant.MIN_TIME);
      case SUM:
      case MIN_VALUE:
      case MAX_VALUE:
      case EXTREME:
      case COUNT:
      case COUNT_TIME:
      case MIN_TIME:
      case MAX_TIME:
      case COUNT_IF:
      case MODE:
        return Collections.emptyList();
      default:
        throw new IllegalArgumentException(
            String.format("Invalid Aggregation function: %s", aggregationType));
    }
  }

  public static boolean isUsingSameColumn(TSDataType originalDataType, TSDataType dataType) {
    if (originalDataType == dataType) {
      return true;
    }
    return Objects.equals(
        dataTypeColumnClassMap.get(originalDataType), dataTypeColumnClassMap.get(dataType));
  }

  public static void changeMetadataModified(
      TimeseriesMetadata timeseriesMetadata, TSDataType targetDataType) {
    if (timeseriesMetadata == null) {
      return;
    }
    if (!SchemaUtils.isUsingSameColumn(timeseriesMetadata.getTsDataType(), targetDataType)
        && ((targetDataType == TSDataType.STRING) || (targetDataType == TSDataType.TEXT))) {
      timeseriesMetadata.setModified(true);
      List<IChunkMetadata> chunkMetadataList = timeseriesMetadata.getChunkMetadataList();
      if (chunkMetadataList != null) {
        for (IChunkMetadata chunkMetadata : chunkMetadataList) {
          if (chunkMetadata != null) {
            chunkMetadata.setModified(true);
          }
        }
      }
    }
  }

  public static void changeAlignedMetadataModified(
      AbstractAlignedTimeSeriesMetadata alignedTimeSeriesMetadata,
      List<TSDataType> targetDataTypeList) {
    if (alignedTimeSeriesMetadata == null) {
      return;
    }

    int i = 0;
    for (TimeseriesMetadata timeseriesMetadata :
        alignedTimeSeriesMetadata.getValueTimeseriesMetadataList()) {
      if ((timeseriesMetadata != null)
          && !SchemaUtils.isUsingSameColumn(
              timeseriesMetadata.getTsDataType(), targetDataTypeList.get(i))
          && ((targetDataTypeList.get(i) == TSDataType.STRING)
              || (targetDataTypeList.get(i) == TSDataType.TEXT))) {
        timeseriesMetadata.setModified(true);
        alignedTimeSeriesMetadata.setModified(true);
        List<IChunkMetadata> chunkMetadataList = timeseriesMetadata.getChunkMetadataList();
        if (chunkMetadataList != null) {
          for (IChunkMetadata chunkMetadata : chunkMetadataList) {
            if (chunkMetadata != null) {
              chunkMetadata.setModified(true);
            }
          }
        }
      }
      i++;
    }
  }

  public static void changeAlignedMetadataModified(
      TimeseriesMetadata timeseriesMetadata, TSDataType targetDataType) {
    if (timeseriesMetadata == null) {
      return;
    }

    if (!SchemaUtils.isUsingSameColumn(timeseriesMetadata.getTsDataType(), targetDataType)
        && ((targetDataType == TSDataType.STRING) || (targetDataType == TSDataType.TEXT))) {
      timeseriesMetadata.setModified(true);
      List<IChunkMetadata> chunkMetadataList = timeseriesMetadata.getChunkMetadataList();
      if (chunkMetadataList != null) {
        for (IChunkMetadata chunkMetadata : chunkMetadataList) {
          if (chunkMetadata != null) {
            chunkMetadata.setModified(true);
          }
        }
      }
    }
  }

  public static void changeMetadataModified(
      IChunkMetadata chunkMetadata, TSDataType sourceDataType, TSDataType targetDataType) {
    if (chunkMetadata == null) {
      return;
    }
    if (!SchemaUtils.isUsingSameColumn(sourceDataType, targetDataType)
        && ((targetDataType == TSDataType.STRING) || (targetDataType == TSDataType.TEXT))) {
      chunkMetadata.setModified(true);
    }
  }

  public static void changeAlignedMetadataModified(
      AbstractAlignedChunkMetadata chunkMetadata,
      TSDataType sourceDataType,
      List<TSDataType> targetDataTypeList) {
    if (chunkMetadata == null) {
      return;
    }
    int i = 0;
    for (IChunkMetadata iChunkMetadata : chunkMetadata.getValueChunkMetadataList()) {
      if ((iChunkMetadata != null)
          && !SchemaUtils.isUsingSameColumn(sourceDataType, targetDataTypeList.get(i))
          && ((targetDataTypeList.get(i) == TSDataType.STRING)
              || (targetDataTypeList.get(i) == TSDataType.TEXT))) {
        iChunkMetadata.setModified(true);
        chunkMetadata.setModified(true);
      }
      i++;
    }
  }

  public static void rewriteAlignedChunkMetadataStatistics(
      AbstractAlignedChunkMetadata alignedChunkMetadata, int index, TSDataType targetDataType) {
    IChunkMetadata valueChunkMetadata = alignedChunkMetadata.getValueChunkMetadataList().get(index);
    if (valueChunkMetadata != null
        && targetDataType.isCompatible(valueChunkMetadata.getDataType())) {
      Statistics<?> statistics = Statistics.getStatsByType(targetDataType);
      statistics = getNewStatistics(valueChunkMetadata, targetDataType, statistics);

      ChunkMetadata newChunkMetadata = (ChunkMetadata) valueChunkMetadata;
      newChunkMetadata.setTsDataType(targetDataType);
      newChunkMetadata.setStatistics(statistics);
    } else {
      alignedChunkMetadata.getValueChunkMetadataList().set(index, null);
    }
  }

  public static void rewriteNonAlignedChunkMetadataStatistics(
      List<IChunkMetadata> chunkMetadataList, int index, TSDataType targetDataType) {
    ChunkMetadata chunkMetadata = (ChunkMetadata) chunkMetadataList.get(index);
    if (chunkMetadata != null && targetDataType.isCompatible(chunkMetadata.getDataType())) {
      Statistics<?> statistics = Statistics.getStatsByType(targetDataType);
      statistics = getNewStatistics(chunkMetadata, targetDataType, statistics);

      chunkMetadata.setTsDataType(targetDataType);
      chunkMetadata.setStatistics(statistics);
    } else {
      chunkMetadataList.set(index, null);
    }
  }

  public static TSEncoding getDataTypeCompatibleEncoding(TSDataType dataType, TSEncoding encoding) {
    if (!encoding.isSupported(dataType)) {
      return EncodingInferenceUtils.getDefaultEncoding(dataType);
    }
    return encoding;
  }

  public static Statistics<?> getNewStatistics(
      IChunkMetadata chunkMetadata, TSDataType targetDataType, Statistics<?> statistics) {
    switch (chunkMetadata.getDataType()) {
      case INT32:
      case INT64:
      case TIMESTAMP:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
        if (targetDataType == TSDataType.STRING) {
          Binary[] binaryValues = new Binary[4];
          binaryValues[0] =
              new Binary(
                  chunkMetadata.getStatistics().getFirstValue().toString(), StandardCharsets.UTF_8);
          binaryValues[1] =
              new Binary(
                  chunkMetadata.getStatistics().getLastValue().toString(), StandardCharsets.UTF_8);
          if (chunkMetadata.getDataType() == TSDataType.BOOLEAN) {
            binaryValues[2] = new Binary(Boolean.FALSE.toString(), StandardCharsets.UTF_8);
            binaryValues[3] = new Binary(Boolean.TRUE.toString(), StandardCharsets.UTF_8);
          } else {
            binaryValues[2] =
                new Binary(
                    chunkMetadata.getStatistics().getMinValue().toString(), StandardCharsets.UTF_8);
            binaryValues[3] =
                new Binary(
                    chunkMetadata.getStatistics().getMaxValue().toString(), StandardCharsets.UTF_8);
          }
          long[] longValues = new long[4];
          longValues[0] = chunkMetadata.getStatistics().getStartTime();
          longValues[1] = chunkMetadata.getStatistics().getEndTime();
          longValues[2] = longValues[1];
          longValues[3] = longValues[1];
          statistics.update(longValues, binaryValues, binaryValues.length);
        } else if (targetDataType == TSDataType.TEXT) {
          Binary[] binaryValues = new Binary[2];
          if (chunkMetadata.getDataType() == TSDataType.BOOLEAN) {
            binaryValues[0] = new Binary(Boolean.FALSE.toString(), StandardCharsets.UTF_8);
            binaryValues[1] = new Binary(Boolean.TRUE.toString(), StandardCharsets.UTF_8);
          } else {
            binaryValues[0] =
                new Binary(
                    chunkMetadata.getStatistics().getMinValue().toString(), StandardCharsets.UTF_8);
            binaryValues[1] =
                new Binary(
                    chunkMetadata.getStatistics().getMaxValue().toString(), StandardCharsets.UTF_8);
          }
          long[] longValues = new long[2];
          longValues[0] = chunkMetadata.getStatistics().getStartTime();
          longValues[1] = chunkMetadata.getStatistics().getEndTime();
          statistics.update(longValues, binaryValues, binaryValues.length);
        } else {
          statistics = chunkMetadata.getStatistics();
        }
        break;
      case DATE:
        if (targetDataType == TSDataType.STRING) {
          Binary[] binaryValues = new Binary[4];
          binaryValues[0] =
              new Binary(
                  TSDataType.getDateStringValue(
                      (Integer) chunkMetadata.getStatistics().getFirstValue()),
                  StandardCharsets.UTF_8);
          binaryValues[1] =
              new Binary(
                  TSDataType.getDateStringValue(
                      (Integer) chunkMetadata.getStatistics().getLastValue()),
                  StandardCharsets.UTF_8);
          binaryValues[2] =
              new Binary(
                  TSDataType.getDateStringValue(
                      (Integer) chunkMetadata.getStatistics().getMinValue()),
                  StandardCharsets.UTF_8);
          binaryValues[3] =
              new Binary(
                  TSDataType.getDateStringValue(
                      (Integer) chunkMetadata.getStatistics().getMaxValue()),
                  StandardCharsets.UTF_8);
          long[] longValues = new long[4];
          longValues[0] = chunkMetadata.getStatistics().getStartTime();
          longValues[1] = chunkMetadata.getStatistics().getEndTime();
          longValues[2] = longValues[1];
          longValues[3] = longValues[1];
          statistics.update(longValues, binaryValues, binaryValues.length);
        } else if (targetDataType == TSDataType.TEXT) {
          Binary[] binaryValues = new Binary[2];
          binaryValues[0] =
              new Binary(
                  TSDataType.getDateStringValue(
                      (Integer) chunkMetadata.getStatistics().getFirstValue()),
                  StandardCharsets.UTF_8);
          binaryValues[1] =
              new Binary(
                  TSDataType.getDateStringValue(
                      (Integer) chunkMetadata.getStatistics().getLastValue()),
                  StandardCharsets.UTF_8);
          long[] longValues = new long[2];
          longValues[0] = chunkMetadata.getStatistics().getStartTime();
          longValues[1] = chunkMetadata.getStatistics().getEndTime();
          statistics.update(longValues, binaryValues, binaryValues.length);
        }
        break;
      case STRING:
        if (targetDataType == TSDataType.TEXT) {
          Binary[] binaryValues = new Binary[2];
          binaryValues[0] =
              new Binary(
                  chunkMetadata.getStatistics().getMinValue().toString(), StandardCharsets.UTF_8);
          binaryValues[1] =
              new Binary(
                  chunkMetadata.getStatistics().getMaxValue().toString(), StandardCharsets.UTF_8);
          long[] longValues = new long[2];
          longValues[0] = chunkMetadata.getStatistics().getStartTime();
          longValues[1] = chunkMetadata.getStatistics().getEndTime();
          statistics.update(longValues, binaryValues, binaryValues.length);
        } else if (targetDataType == TSDataType.BLOB) {
          statistics.update(
              chunkMetadata.getStatistics().getStartTime(),
              new Binary(
                  chunkMetadata.getStatistics().getMinValue().toString(), StandardCharsets.UTF_8));
          statistics.update(
              chunkMetadata.getStatistics().getEndTime(),
              new Binary(
                  chunkMetadata.getStatistics().getMaxValue().toString(), StandardCharsets.UTF_8));
        } else {
          statistics = chunkMetadata.getStatistics();
        }
        break;
      case TEXT:
        if (targetDataType == TSDataType.STRING) {
          Binary[] binaryValues = new Binary[2];
          binaryValues[0] = (Binary) chunkMetadata.getStatistics().getFirstValue();
          binaryValues[1] = (Binary) chunkMetadata.getStatistics().getLastValue();
          long[] longValues = new long[2];
          longValues[0] = chunkMetadata.getStatistics().getStartTime();
          longValues[1] = chunkMetadata.getStatistics().getEndTime();
          statistics.update(longValues, binaryValues, binaryValues.length);
        } else if (targetDataType == TSDataType.BLOB) {
          statistics.update(chunkMetadata.getStatistics().getStartTime(), EMPTY_BINARY);
          statistics.update(chunkMetadata.getStatistics().getEndTime(), EMPTY_BINARY);
        } else {
          statistics = chunkMetadata.getStatistics();
        }
        break;
      case BLOB:
        if (targetDataType == TSDataType.STRING || targetDataType == TSDataType.TEXT) {
          Binary[] binaryValues = new Binary[2];
          binaryValues[0] = EMPTY_BINARY;
          binaryValues[1] = EMPTY_BINARY;
          long[] longValues = new long[2];
          longValues[0] = chunkMetadata.getStatistics().getStartTime();
          longValues[1] = chunkMetadata.getStatistics().getEndTime();
          statistics.update(longValues, binaryValues, binaryValues.length);
        } else {
          statistics = chunkMetadata.getStatistics();
        }
        break;
      default:
        break;
    }
    return statistics;
  }
}
