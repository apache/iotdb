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

package org.apache.iotdb.rpc;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsResp;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionResp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Proxy;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class RpcUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(RpcUtils.class);

  /** How big should the default read and write buffers be? Defaults to 1KB */
  public static final int THRIFT_DEFAULT_BUF_CAPACITY = 1024;

  /**
   * It is used to prevent the size of the parsing package from being too large and allocating the
   * buffer will cause oom. Therefore, the maximum length of the requested memory is limited when
   * reading. Thrift max frame size (16384000 bytes by default), we change it to 512MB.
   */
  public static final int THRIFT_FRAME_MAX_SIZE = 536870912;

  /**
   * if resizeIfNecessary is called continuously with a small size for more than
   * MAX_BUFFER_OVERSIZE_TIME times, we will shrink the buffer to reclaim space.
   */
  public static final int MAX_BUFFER_OVERSIZE_TIME = 5;

  public static final long MIN_SHRINK_INTERVAL = 60_000L;

  public static final String TIME_PRECISION = "timestamp_precision";

  public static final String MILLISECOND = "ms";

  public static final String MICROSECOND = "us";

  public static final String NANOSECOND = "ns";

  private RpcUtils() {
    // util class
  }

  public static final TSStatus SUCCESS_STATUS =
      new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());

  public static IClientRPCService.Iface newSynchronizedClient(IClientRPCService.Iface client) {
    return (IClientRPCService.Iface)
        Proxy.newProxyInstance(
            RpcUtils.class.getClassLoader(),
            new Class[] {IClientRPCService.Iface.class},
            new SynchronizedHandler(client));
  }

  /**
   * verify success.
   *
   * @param status -status
   */
  public static void verifySuccess(TSStatus status) throws StatementExecutionException {
    if (status.getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
      verifySuccess(status.getSubStatus());
      return;
    }
    if (status.getCode() == TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
      return;
    }
    if (status.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new StatementExecutionException(status);
    }
  }

  public static void verifySuccessWithRedirection(TSStatus status)
      throws StatementExecutionException, RedirectException {
    verifySuccess(status);
    if (status.isSetRedirectNode()) {
      throw new RedirectException(status.getRedirectNode());
    }
  }

  public static void verifySuccessWithRedirectionForMultiDevices(
      TSStatus status, List<String> devices) throws StatementExecutionException, RedirectException {
    verifySuccess(status);
    if (status.getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode()
        || status.getCode() == TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
      Map<String, TEndPoint> deviceEndPointMap = new HashMap<>();
      List<TSStatus> statusSubStatus = status.getSubStatus();
      for (int i = 0; i < statusSubStatus.size(); i++) {
        TSStatus subStatus = statusSubStatus.get(i);
        if (subStatus.isSetRedirectNode()) {
          deviceEndPointMap.put(devices.get(i), subStatus.getRedirectNode());
        }
      }
      throw new RedirectException(deviceEndPointMap);
    }
  }

  public static void verifySuccess(List<TSStatus> statuses) throws BatchExecutionException {
    StringBuilder errMsgs =
        new StringBuilder().append(TSStatusCode.MULTIPLE_ERROR.getStatusCode()).append(": ");
    for (TSStatus status : statuses) {
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
          && status.getCode() != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
        errMsgs.append(status.getMessage()).append("; ");
      }
    }
    if (errMsgs.length() > 0) {
      throw new BatchExecutionException(statuses, errMsgs.toString());
    }
  }

  /**
   * Convert from {@link TSStatusCode} to {@link TSStatus} according to status code and status
   * message
   */
  public static TSStatus getStatus(TSStatusCode tsStatusCode) {
    return new TSStatus(tsStatusCode.getStatusCode());
  }

  public static TSStatus getStatus(List<TSStatus> statusList) {
    TSStatus status = new TSStatus(TSStatusCode.MULTIPLE_ERROR.getStatusCode());
    status.setSubStatus(statusList);
    if (LOGGER.isDebugEnabled()) {
      StringBuilder errMsg = new StringBuilder().append("Multiple error occur, messages: ");
      Set<TSStatus> msgSet = new HashSet<>();
      for (TSStatus subStatus : statusList) {
        if (subStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
            && subStatus.getCode() != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
          if (!msgSet.contains(status)) {
            errMsg.append(status).append("; ");
            msgSet.add(status);
          }
        }
      }
      LOGGER.debug(errMsg.toString(), new Exception(errMsg.toString()));
    }
    return status;
  }

  /**
   * Convert from {@link TSStatusCode} to {@link TSStatus}, which has message appended with existing
   * status message
   *
   * @param tsStatusCode status type
   * @param message appending message
   */
  public static TSStatus getStatus(TSStatusCode tsStatusCode, String message) {
    TSStatus status = new TSStatus(tsStatusCode.getStatusCode());
    status.setMessage(message);
    return status;
  }

  public static TSStatus getStatus(int code, String message) {
    TSStatus status = new TSStatus(code);
    status.setMessage(message);
    return status;
  }

  public static TSExecuteStatementResp getTSExecuteStatementResp(TSStatusCode tsStatusCode) {
    TSStatus status = getStatus(tsStatusCode);
    return getTSExecuteStatementResp(status);
  }

  public static TSExecuteStatementResp getTSExecuteStatementResp(
      TSStatusCode tsStatusCode, String message) {
    TSStatus status = getStatus(tsStatusCode, message);
    return getTSExecuteStatementResp(status);
  }

  public static TSExecuteStatementResp getTSExecuteStatementResp(TSStatus status) {
    TSExecuteStatementResp resp = new TSExecuteStatementResp();
    TSStatus tsStatus = new TSStatus(status);
    resp.setStatus(tsStatus);
    return resp;
  }

  public static TSFetchResultsResp getTSFetchResultsResp(TSStatusCode tsStatusCode) {
    TSStatus status = getStatus(tsStatusCode);
    return getTSFetchResultsResp(status);
  }

  public static TSFetchResultsResp getTSFetchResultsResp(
      TSStatusCode tsStatusCode, String appendMessage) {
    TSStatus status = getStatus(tsStatusCode, appendMessage);
    return getTSFetchResultsResp(status);
  }

  public static TSFetchResultsResp getTSFetchResultsResp(TSStatus status) {
    TSFetchResultsResp resp = new TSFetchResultsResp();
    TSStatus tsStatus = new TSStatus(status);
    resp.setStatus(tsStatus);
    return resp;
  }

  public static final String DEFAULT_TIME_FORMAT = "default";
  public static final String DEFAULT_TIMESTAMP_PRECISION = "ms";

  public static String setTimeFormat(String newTimeFormat) {
    String timeFormat;
    switch (newTimeFormat.trim().toLowerCase()) {
      case "long":
      case "number":
      case DEFAULT_TIME_FORMAT:
      case "iso8601":
        timeFormat = newTimeFormat.trim().toLowerCase();
        break;
      default:
        // use java default SimpleDateFormat to check whether input time format is legal
        // if illegal, it will throw an exception
        new SimpleDateFormat(newTimeFormat.trim());
        timeFormat = newTimeFormat;
        break;
    }
    return timeFormat;
  }

  public static String formatDatetime(
      String timeFormat, String timePrecision, long timestamp, ZoneId zoneId) {
    ZonedDateTime dateTime;
    switch (timeFormat) {
      case "long":
      case "number":
        return Long.toString(timestamp);
      case DEFAULT_TIME_FORMAT:
      case "iso8601":
        return parseLongToDateWithPrecision(
            DateTimeFormatter.ISO_OFFSET_DATE_TIME, timestamp, zoneId, timePrecision);
      default:
        dateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), zoneId);
        return dateTime.format(DateTimeFormatter.ofPattern(timeFormat));
    }
  }

  public static String formatDatetimeStr(String datetime, StringBuilder digits) {
    if (datetime.contains("+")) {
      String timeZoneStr = datetime.substring(datetime.length() - 6);
      return datetime.substring(0, datetime.length() - 6) + "." + digits + timeZoneStr;
    } else if (datetime.contains("Z")) {
      String timeZoneStr = datetime.substring(datetime.length() - 1);
      return datetime.substring(0, datetime.length() - 1) + "." + digits + timeZoneStr;
    } else {
      String timeZoneStr = "";
      return datetime + "." + digits + timeZoneStr;
    }
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public static String parseLongToDateWithPrecision(
      DateTimeFormatter formatter, long timestamp, ZoneId zoneid, String timestampPrecision) {
    long integerOfDate;
    StringBuilder digits;
    if (MILLISECOND.equals(timestampPrecision)) {
      if (timestamp > 0 || timestamp % 1000 == 0) {
        integerOfDate = timestamp / 1000;
        digits = new StringBuilder(Long.toString(timestamp % 1000));
      } else {
        integerOfDate = timestamp / 1000 - 1;
        digits = new StringBuilder(Long.toString(1000 + timestamp % 1000));
      }
      ZonedDateTime dateTime =
          ZonedDateTime.ofInstant(Instant.ofEpochSecond(integerOfDate), zoneid);
      String datetime = dateTime.format(formatter);
      int length = digits.length();
      if (length != 3) {
        for (int i = 0; i < 3 - length; i++) {
          digits.insert(0, "0");
        }
      }
      return formatDatetimeStr(datetime, digits);
    } else if (MICROSECOND.equals(timestampPrecision)) {
      if (timestamp > 0 || timestamp % 1000_000 == 0) {
        integerOfDate = timestamp / 1000_000;
        digits = new StringBuilder(Long.toString(timestamp % 1000_000));
      } else {
        integerOfDate = timestamp / 1000_000 - 1;
        digits = new StringBuilder(Long.toString(1000_000 + timestamp % 1000_000));
      }
      ZonedDateTime dateTime =
          ZonedDateTime.ofInstant(Instant.ofEpochSecond(integerOfDate), zoneid);
      String datetime = dateTime.format(formatter);
      int length = digits.length();
      if (length != 6) {
        for (int i = 0; i < 6 - length; i++) {
          digits.insert(0, "0");
        }
      }
      return formatDatetimeStr(datetime, digits);
    } else {
      if (timestamp > 0 || timestamp % 1000_000_000L == 0) {
        integerOfDate = timestamp / 1000_000_000L;
        digits = new StringBuilder(Long.toString(timestamp % 1000_000_000L));
      } else {
        integerOfDate = timestamp / 1000_000_000L - 1;
        digits = new StringBuilder(Long.toString(1000_000_000L + timestamp % 1000_000_000L));
      }
      ZonedDateTime dateTime =
          ZonedDateTime.ofInstant(Instant.ofEpochSecond(integerOfDate), zoneid);
      String datetime = dateTime.format(formatter);
      int length = digits.length();
      if (length != 9) {
        for (int i = 0; i < 9 - length; i++) {
          digits.insert(0, "0");
        }
      }
      return formatDatetimeStr(datetime, digits);
    }
  }

  public static TSStatus squashResponseStatusList(List<TSStatus> responseStatusList) {
    final List<TSStatus> failedStatus =
        responseStatusList.stream()
            .filter(status -> status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode())
            .collect(Collectors.toList());
    return failedStatus.isEmpty()
        ? new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode())
        : new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
            .setMessage(failedStatus.toString());
  }

  public static boolean isUseDatabase(String sql) {
    return sql.length() > 4 && "use ".equalsIgnoreCase(sql.substring(0, 4));
  }

  public static long getMilliSecond(long time, int timeFactor) {
    return time / timeFactor * 1_000;
  }

  public static int getNanoSecond(long time, int timeFactor) {
    return (int) (time % timeFactor * (1_000_000_000 / timeFactor));
  }

  public static Timestamp convertToTimestamp(long time, int timeFactor) {
    Timestamp res = new Timestamp(getMilliSecond(time, timeFactor));
    res.setNanos(getNanoSecond(time, timeFactor));
    return res;
  }

  public static int getTimeFactor(TSOpenSessionResp openResp) {
    if (openResp.isSetConfiguration()) {
      String precision = openResp.getConfiguration().get(TIME_PRECISION);
      if (precision != null) {
        switch (precision) {
          case MILLISECOND:
            return 1_000;
          case MICROSECOND:
            return 1_000_000;
          case NANOSECOND:
            return 1_000_000_000;
          default:
            throw new IllegalArgumentException("Unknown time precision: " + precision);
        }
      }
    }
    return 1_000;
  }

  public static String getTimePrecision(int timeFactor) {
    switch (timeFactor) {
      case 1_000:
        return MILLISECOND;
      case 1_000_000:
        return MICROSECOND;
      case 1_000_000_000:
        return NANOSECOND;
      default:
        throw new IllegalArgumentException("Unknown time factor: " + timeFactor);
    }
  }
}
