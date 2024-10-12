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

import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.protocol.thrift.OperationType;
import org.apache.iotdb.db.queryengine.plan.execution.IQueryExecution;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.literal.BinaryLiteral;
import org.apache.iotdb.db.utils.constant.SqlConstant;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.service.rpc.thrift.TSAggregationQueryReq;
import org.apache.iotdb.service.rpc.thrift.TSFastLastDataQueryForOneDeviceReq;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsReq;
import org.apache.iotdb.service.rpc.thrift.TSLastDataQueryReq;
import org.apache.iotdb.service.rpc.thrift.TSRawDataQueryReq;

import com.google.common.base.Throwables;
import io.airlift.airline.Cli;
import io.airlift.airline.Help;
import io.airlift.airline.ParseArgumentsMissingException;
import io.airlift.airline.ParseArgumentsUnexpectedException;
import io.airlift.airline.ParseCommandMissingException;
import io.airlift.airline.ParseCommandUnrecognizedException;
import io.airlift.airline.ParseOptionConversionException;
import io.airlift.airline.ParseOptionMissingException;
import io.airlift.airline.ParseOptionMissingValueException;
import org.apache.commons.lang3.StringUtils;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("java:S106") // for console outputs
public class CommonUtils {

  private static final int MAX_SLOW_NATIVE_API_OUTPUT_NUM = 10;

  private static final String UNKNOWN_RESULT = "UNKNOWN";

  private CommonUtils() {}

  public static Object parseValue(TSDataType dataType, String value) throws QueryProcessException {
    return parseValue(dataType, value, ZoneId.systemDefault());
  }

  public static Object parseValue(TSDataType dataType, String value, ZoneId zoneId)
      throws QueryProcessException {
    try {
      if ("null".equals(value) || "NULL".equals(value)) {
        return null;
      }
      switch (dataType) {
        case BOOLEAN:
          return parseBoolean(value);
        case INT32:
          try {
            return Integer.parseInt(StringUtils.trim(value));
          } catch (NumberFormatException e) {
            throw new NumberFormatException(
                "data type is not consistent, input " + value + ", registered " + dataType);
          }
        case INT64:
          try {
            return Long.parseLong(StringUtils.trim(value));
          } catch (NumberFormatException e) {
            throw new NumberFormatException(
                "data type is not consistent, input " + value + ", registered " + dataType);
          }
        case TIMESTAMP:
          try {
            if (TypeInferenceUtils.isNumber(value)) {
              return Long.parseLong(value);
            } else {
              return DateTimeUtils.parseDateTimeExpressionToLong(StringUtils.trim(value), zoneId);
            }
          } catch (Throwable e) {
            throw new NumberFormatException(
                "data type is not consistent, input "
                    + value
                    + ", registered "
                    + dataType
                    + " because "
                    + e.getMessage());
          }
        case DATE:
          return parseIntFromString(value);
        case FLOAT:
          float f;
          try {
            f = Float.parseFloat(value);
          } catch (NumberFormatException e) {
            throw new NumberFormatException(
                "data type is not consistent, input " + value + ", registered " + dataType);
          }
          if (Float.isInfinite(f)) {
            throw new NumberFormatException("The input float value is Infinity");
          }
          return f;
        case DOUBLE:
          double d;
          try {
            d = Double.parseDouble(value);
          } catch (NumberFormatException e) {
            throw new NumberFormatException(
                "data type is not consistent, input " + value + ", registered " + dataType);
          }
          if (Double.isInfinite(d)) {
            throw new NumberFormatException("The input double value is Infinity");
          }
          return d;
        case TEXT:
        case STRING:
          if ((value.startsWith(SqlConstant.QUOTE) && value.endsWith(SqlConstant.QUOTE))
              || (value.startsWith(SqlConstant.DQUOTE) && value.endsWith(SqlConstant.DQUOTE))) {
            if (value.length() == 1) {
              return new Binary(value, TSFileConfig.STRING_CHARSET);
            } else {
              return new Binary(
                  value.substring(1, value.length() - 1), TSFileConfig.STRING_CHARSET);
            }
          }
          return new Binary(value, TSFileConfig.STRING_CHARSET);
        case BLOB:
          if ((value.startsWith(SqlConstant.QUOTE) && value.endsWith(SqlConstant.QUOTE))
              || (value.startsWith(SqlConstant.DQUOTE) && value.endsWith(SqlConstant.DQUOTE))) {
            if (value.length() == 1) {
              return new Binary(parseBlobStringToByteArray(value));
            } else {
              return new Binary(parseBlobStringToByteArray(value.substring(1, value.length() - 1)));
            }
          }
          return new Binary(parseBlobStringToByteArray(value));
        default:
          throw new QueryProcessException("Unsupported data type:" + dataType);
      }
    } catch (NumberFormatException e) {
      throw new QueryProcessException(e.getMessage());
    }
  }

  public static Integer parseIntFromString(String value) {
    try {
      if (value.length() == 12
          && ((value.startsWith(SqlConstant.QUOTE) && value.endsWith(SqlConstant.QUOTE))
              || (value.startsWith(SqlConstant.DQUOTE) && value.endsWith(SqlConstant.DQUOTE)))) {
        return DateTimeUtils.parseDateExpressionToInt(value.substring(1, value.length() - 1));
      } else {
        return DateTimeUtils.parseDateExpressionToInt(StringUtils.trim(value));
      }
    } catch (Throwable e) {
      throw new NumberFormatException(
          "data type is not consistent, input "
              + value
              + ", registered "
              + TSDataType.DATE
              + " because "
              + e.getMessage());
    }
  }

  public static boolean checkCanCastType(TSDataType src, TSDataType dest) {
    if (Objects.isNull(src)) {
      return true;
    }
    switch (src) {
      case INT32:
        if (dest == TSDataType.INT64 || dest == TSDataType.FLOAT || dest == TSDataType.DOUBLE) {
          return true;
        }
      case INT64:
        if (dest == TSDataType.DOUBLE) {
          return true;
        }
      case FLOAT:
        if (dest == TSDataType.DOUBLE) {
          return true;
        }
    }
    return false;
  }

  public static Object castValue(TSDataType srcDataType, TSDataType destDataType, Object value) {
    if (Objects.isNull(value)) {
      return null;
    }
    switch (srcDataType) {
      case INT32:
        if (destDataType == TSDataType.INT64) {
          value = (long) ((int) value);
        } else if (destDataType == TSDataType.FLOAT) {
          value = (float) ((int) value);
        } else if (destDataType == TSDataType.DOUBLE) {
          value = (double) ((int) value);
        }
        break;
      case INT64:
        if (destDataType == TSDataType.DOUBLE) {
          value = (double) ((long) value);
        }
        break;
      case FLOAT:
        if (destDataType == TSDataType.DOUBLE) {
          value = (double) ((float) value);
        }
        break;
    }
    return value;
  }

  public static Object castArray(TSDataType srcDataType, TSDataType destDataType, Object value) {
    switch (srcDataType) {
      case INT32:
        if (destDataType == TSDataType.INT64) {
          value = Arrays.stream((int[]) value).mapToLong(Long::valueOf).toArray();
        } else if (destDataType == TSDataType.FLOAT) {
          int[] tmp = (int[]) value;
          float[] result = new float[tmp.length];
          for (int i = 0; i < tmp.length; i++) {
            result[i] = (float) tmp[i];
          }
          value = result;
        } else if (destDataType == TSDataType.DOUBLE) {
          value = Arrays.stream((int[]) value).mapToDouble(Double::valueOf).toArray();
        }
        break;
      case INT64:
        if (destDataType == TSDataType.DOUBLE) {
          value = Arrays.stream((long[]) value).mapToDouble(Double::valueOf).toArray();
        }
        break;
      case FLOAT:
        if (destDataType == TSDataType.DOUBLE) {
          float[] tmp = (float[]) value;
          double[] result = new double[tmp.length];
          for (int i = 0; i < tmp.length; i++) {
            result[i] = tmp[i];
          }
          value = result;
        }
        break;
    }
    return value;
  }

  private static boolean parseBoolean(String value) throws QueryProcessException {
    value = value.toLowerCase();
    if (SqlConstant.BOOLEAN_FALSE_NUM.equals(value) || SqlConstant.BOOLEAN_FALSE.equals(value)) {
      return false;
    }
    if (SqlConstant.BOOLEAN_TRUE_NUM.equals(value) || SqlConstant.BOOLEAN_TRUE.equals(value)) {
      return true;
    }
    throw new QueryProcessException("The BOOLEAN should be true/TRUE, false/FALSE or 0/1");
  }

  public static String getContentOfRequest(
      org.apache.thrift.TBase<?, ?> request, IQueryExecution queryExecution) {
    if (queryExecution == null) {
      return UNKNOWN_RESULT;
    }

    String executeSql = queryExecution.getExecuteSQL().orElse("");
    if (!executeSql.isEmpty()) {
      return executeSql;
    } else if (request == null) {
      return UNKNOWN_RESULT;
    } else if (request instanceof TSRawDataQueryReq) {
      TSRawDataQueryReq req = (TSRawDataQueryReq) request;
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < Math.min(req.getPathsSize(), MAX_SLOW_NATIVE_API_OUTPUT_NUM); i++) {
        sb.append(i == 0 ? "" : ",").append(req.getPaths().get(i));
      }
      return String.format(
          "Request name: TSRawDataQueryReq, paths size: %s, starTime: %s, "
              + "endTime: %s, some paths: %s",
          req.getPathsSize(), req.getStartTime(), req.getEndTime(), sb);
    } else if (request instanceof TSLastDataQueryReq) {
      TSLastDataQueryReq req = (TSLastDataQueryReq) request;
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < Math.min(req.getPathsSize(), MAX_SLOW_NATIVE_API_OUTPUT_NUM); i++) {
        sb.append(i == 0 ? "" : ",").append(req.getPaths().get(i));
      }
      return String.format(
          "Request name: TSLastDataQueryReq, paths size: %s, some paths: %s",
          req.getPathsSize(), sb);
    } else if (request instanceof TSAggregationQueryReq) {
      TSAggregationQueryReq req = (TSAggregationQueryReq) request;
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < Math.min(req.getPathsSize(), MAX_SLOW_NATIVE_API_OUTPUT_NUM); i++) {
        sb.append(i == 0 ? "" : ",")
            .append(req.getAggregations().get(i))
            .append(":")
            .append(req.getPaths().get(i));
      }
      return String.format(
          "Request name: TSAggregationQueryReq, startTime: %s, endTime: %s, "
              + "paths size: %s, some paths: %s",
          req.getStartTime(), req.getEndTime(), req.getPathsSize(), sb);
    } else if (request instanceof TSFastLastDataQueryForOneDeviceReq) {
      TSFastLastDataQueryForOneDeviceReq req = (TSFastLastDataQueryForOneDeviceReq) request;
      return String.format(
          "Request name: TSFastLastDataQueryForOneDeviceReq, "
              + "db: %s, deviceId: %s, sensorSize: %s, sensors: %s",
          req.getDb(), req.getDeviceId(), req.getSensorsSize(), req.getSensors());
    } else if (request instanceof TSFetchResultsReq) {
      TSFetchResultsReq req = (TSFetchResultsReq) request;
      StringBuilder sb = new StringBuilder();
      for (int i = 0;
          i < Math.min(queryExecution.getOutputValueColumnCount(), MAX_SLOW_NATIVE_API_OUTPUT_NUM);
          i++) {
        sb.append(i == 0 ? "" : ",")
            .append(queryExecution.getDatasetHeader().getRespColumns().get(i));
      }
      return String.format(
          "Request name: TSFetchResultsReq, "
              + "queryId: %s, output value column count: %s, fetchSize: %s, "
              + "some response headers: %s",
          req.getQueryId(), queryExecution.getOutputValueColumnCount(), req.getFetchSize(), sb);
    } else {
      return UNKNOWN_RESULT;
    }
  }

  public static int runCli(
      List<Class<? extends Runnable>> commands,
      String[] args,
      String cliName,
      String cliDescription) {
    Cli.CliBuilder<Runnable> builder = Cli.builder(cliName);

    builder.withDescription(cliDescription).withDefaultCommand(Help.class).withCommands(commands);

    Cli<Runnable> parser = builder.build();

    int status = 0;
    try {
      Runnable parse = parser.parse(args);
      parse.run();
    } catch (IllegalArgumentException
        | IllegalStateException
        | ParseArgumentsMissingException
        | ParseArgumentsUnexpectedException
        | ParseOptionConversionException
        | ParseOptionMissingException
        | ParseOptionMissingValueException
        | ParseCommandMissingException
        | ParseCommandUnrecognizedException e) {
      badUse(e);
      status = 1;
    } catch (Exception e) {
      err(Throwables.getRootCause(e));
      status = 2;
    }
    return status;
  }

  /**
   * Converts a string into a byte array based on the encoding format (hex or escape).
   *
   * @param input The input string.
   * @return The encoded byte array.
   * @throws IllegalArgumentException if input is invalid.
   */
  public static byte[] parseBlobStringToByteArray(String input) throws IllegalArgumentException {
    try {
      BinaryLiteral binaryLiteral = new BinaryLiteral(input);
      return binaryLiteral.getValues();
    } catch (SemanticException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
  }

  private static void badUse(Exception e) {
    System.out.println("node-tool: " + e.getMessage());
    System.out.println("See 'node-tool help' or 'node-tool help <command>'.");
  }

  private static void err(Throwable e) {
    System.err.println("error: " + e.getMessage());
    System.err.println("-- StackTrace --");
    System.err.println(Throwables.getStackTraceAsString(e));
  }

  public static String[] deviceIdToStringArray(IDeviceID deviceID) {
    String[] ret = new String[deviceID.segmentNum()];
    for (int i = 0; i < ret.length; i++) {
      ret[i] = deviceID.segment(i) != null ? deviceID.segment(i).toString() : null;
    }
    return ret;
  }

  public static Object[] deviceIdToObjArray(IDeviceID deviceID) {
    Object[] ret = new Object[deviceID.segmentNum()];
    for (int i = 0; i < ret.length; i++) {
      ret[i] = deviceID.segment(i);
    }
    return ret;
  }

  /**
   * Check whether the time falls in TTL.
   *
   * @return whether the given time falls in ttl
   */
  public static boolean isAlive(long time, long dataTTL) {
    return dataTTL == Long.MAX_VALUE || (CommonDateTimeUtils.currentTime() - time) <= dataTTL;
  }

  public static Object createValueColumnOfDataType(
      TSDataType dataType, TsTableColumnCategory columnCategory, int rowNum) {
    Object valueColumn;
    switch (dataType) {
      case INT32:
        valueColumn = new int[rowNum];
        break;
      case INT64:
      case TIMESTAMP:
        valueColumn = new long[rowNum];
        break;
      case FLOAT:
        valueColumn = new float[rowNum];
        break;
      case DOUBLE:
        valueColumn = new double[rowNum];
        break;
      case BOOLEAN:
        valueColumn = new boolean[rowNum];
        break;
      case TEXT:
      case STRING:
        if (columnCategory.equals(TsTableColumnCategory.MEASUREMENT)) {
          valueColumn = new Binary[rowNum];
        } else {
          valueColumn = new String[rowNum];
        }
        break;
      case BLOB:
        valueColumn = new Binary[rowNum];
        break;
      case DATE:
        valueColumn = new LocalDate[rowNum];
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Data type %s is not supported.", dataType));
    }
    return valueColumn;
  }

  public static void swapArray(Object[] array, int i, int j) {
    Object tmp = array[i];
    array[i] = array[j];
    array[j] = tmp;
  }

  /** Add stat of operation into metrics */
  public static void addStatementExecutionLatency(
      OperationType operation, String statementType, long costTime) {
    if (statementType == null) {
      return;
    }

    MetricService.getInstance()
        .timer(
            costTime,
            TimeUnit.NANOSECONDS,
            Metric.PERFORMANCE_OVERVIEW.toString(),
            MetricLevel.CORE,
            Tag.INTERFACE.toString(),
            operation.toString(),
            Tag.TYPE.toString(),
            statementType);
  }

  public static void addQueryLatency(StatementType statementType, long costTimeInNanos) {
    if (statementType == null) {
      return;
    }

    MetricService.getInstance()
        .timer(
            costTimeInNanos,
            TimeUnit.NANOSECONDS,
            Metric.PERFORMANCE_OVERVIEW.toString(),
            MetricLevel.CORE,
            Tag.INTERFACE.toString(),
            OperationType.QUERY_LATENCY.toString(),
            Tag.TYPE.toString(),
            statementType.name());
  }
}
