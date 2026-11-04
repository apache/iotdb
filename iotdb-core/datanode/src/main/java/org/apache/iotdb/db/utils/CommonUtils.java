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

import org.apache.iotdb.calc.exception.QueryProcessException;
import org.apache.iotdb.calc.utils.constant.SqlConstant;
import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.queryengine.utils.DateTimeUtils;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.db.i18n.DataNodeMiscMessages;
import org.apache.iotdb.db.protocol.thrift.OperationType;
import org.apache.iotdb.db.queryengine.plan.execution.IQueryExecution;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.literal.BinaryLiteral;
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
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.external.commons.lang3.StringUtils;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.TimeColumn;
import org.apache.tsfile.read.common.type.Type;

import java.time.ZoneId;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.db.utils.TypeServices.ValueConversion.VALUE_PARSER_SERVICE;

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
      try {
        return VALUE_PARSER_SERVICE.call(Type.fromTsDataType(dataType)).parse(value, zoneId);
      } catch (final UnsupportedOperationException ignored) {
        throw new QueryProcessException(DataNodeMiscMessages.UNSUPPORTED_DATA_TYPE + dataType);
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
          String.format(
              DataNodeMiscMessages
                  .MISC_EXCEPTION_DATA_TYPE_IS_NOT_CONSISTENT_INPUT_S_REGISTERED_S_BECAUSE_50C4BF31,
              value,
              TSDataType.DATE,
              e.getMessage()));
    }
  }

  static boolean parseBoolean(String value) throws QueryProcessException {
    value = value.toLowerCase();
    if (SqlConstant.BOOLEAN_FALSE_NUM.equals(value) || SqlConstant.BOOLEAN_FALSE.equals(value)) {
      return false;
    }
    if (SqlConstant.BOOLEAN_TRUE_NUM.equals(value) || SqlConstant.BOOLEAN_TRUE.equals(value)) {
      return true;
    }
    throw new QueryProcessException(DataNodeMiscMessages.BOOLEAN_PARSE_ERROR);
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
          DataNodeMiscMessages
              .EXCEPTION_REQUEST_NAME_TSRAWDATAQUERYREQ_PATHS_SIZE_ARG_STARTIME_ARG_ENDTIME_ARG_SOME_PATHS_ARG_92B722A4,
          req.getPathsSize(),
          req.getStartTime(),
          req.getEndTime(),
          sb);
    } else if (request instanceof TSLastDataQueryReq) {
      TSLastDataQueryReq req = (TSLastDataQueryReq) request;
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < Math.min(req.getPathsSize(), MAX_SLOW_NATIVE_API_OUTPUT_NUM); i++) {
        sb.append(i == 0 ? "" : ",").append(req.getPaths().get(i));
      }
      return String.format(
          DataNodeMiscMessages
              .EXCEPTION_REQUEST_NAME_TSLASTDATAQUERYREQ_PATHS_SIZE_ARG_SOME_PATHS_ARG_46D12DFF,
          req.getPathsSize(),
          sb);
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
          DataNodeMiscMessages
              .EXCEPTION_REQUEST_NAME_TSAGGREGATIONQUERYREQ_STARTTIME_ARG_ENDTIME_ARG_PATHS_SIZE_ARG_SOME_PATHS_ARG_9E59CBB8,
          req.getStartTime(),
          req.getEndTime(),
          req.getPathsSize(),
          sb);
    } else if (request instanceof TSFastLastDataQueryForOneDeviceReq) {
      TSFastLastDataQueryForOneDeviceReq req = (TSFastLastDataQueryForOneDeviceReq) request;
      return getContentOfTSFastLastDataQueryForOneDeviceReq(req);
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
          DataNodeMiscMessages
              .EXCEPTION_REQUEST_NAME_TSFETCHRESULTSREQ_QUERYID_ARG_OUTPUT_VALUE_COLUMN_COUNT_ARG_FETCHSIZE_ARG_SOME_RESPONSE_HEADERS_ARG_15468822,
          req.getQueryId(),
          queryExecution.getOutputValueColumnCount(),
          req.getFetchSize(),
          sb);
    } else {
      return UNKNOWN_RESULT;
    }
  }

  public static String getContentOfTSFastLastDataQueryForOneDeviceReq(
      TSFastLastDataQueryForOneDeviceReq req) {
    return String.format(
        DataNodeMiscMessages
            .EXCEPTION_REQUEST_NAME_TSFASTLASTDATAQUERYFORONEDEVICEREQ_DB_ARG_DEVICEID_ARG_SENSORSIZE_ARG_SENSORS_ARG_D1F9A498,
        req.getDb(),
        req.getDeviceId(),
        req.getSensorsSize(),
        req.getSensors());
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
    System.out.println(
        String.format(
            DataNodeMiscMessages.LOG_NODE_TOOL_ARG_076E307A, Objects.toString(e.getMessage())));
    System.out.println(
        DataNodeMiscMessages.LOG_SEE_NODE_TOOL_HELP_OR_NODE_TOOL_HELP_COMMAND_BB9F5146);
  }

  private static void err(Throwable e) {
    System.err.println(
        String.format(
            DataNodeMiscMessages.LOG_ERROR_ARG_76028BAA, Objects.toString(e.getMessage())));
    System.err.println(DataNodeMiscMessages.LOG_STACKTRACE_131CBB85);
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
    return dataTTL == Long.MAX_VALUE || time >= getTTLLowerBound(dataTTL);
  }

  public static long getTTLLowerBound(long dataTTL) {
    if (dataTTL == Long.MAX_VALUE) {
      return Long.MIN_VALUE;
    }

    long currentTime = CommonDateTimeUtils.currentTime();
    if (dataTTL >= 0 && currentTime < Long.MIN_VALUE + dataTTL) {
      return Long.MIN_VALUE;
    }
    if (dataTTL < 0 && currentTime > Long.MAX_VALUE + dataTTL) {
      return Long.MAX_VALUE;
    }
    return currentTime - dataTTL;
  }

  public static Object createValueColumnOfDataType(
      TSDataType dataType, TsTableColumnCategory columnCategory, int rowNum) {
    return TypeServices.StorageEngine.TABLET_COLUMN_ALLOCATOR_SERVICE
        .call(Type.fromTsDataType(dataType))
        .apply(rowNum);
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

  public static String toString(TsBlock tsBlock) {
    StringBuilder tsBlockBuilder = new StringBuilder();
    for (Column column : tsBlock.getAllColumns()) {
      tsBlockBuilder.append("[");
      for (int i = 0; i < column.getPositionCount(); i++) {
        if (column instanceof TimeColumn) {
          tsBlockBuilder.append(column.getLong(i)).append(",");
        } else {
          tsBlockBuilder.append(column.getTsPrimitiveType(i)).append(",");
        }
      }
      tsBlockBuilder.append("] ");
    }
    return tsBlockBuilder.toString();
  }
}
