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

package org.apache.iotdb.db.queryengine.udf;

import org.apache.iotdb.calc.plan.planner.TableOperatorGenerator.IoTDBLocalFactory;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.QueryTimeoutException;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.udf.api.IoTDBLocal;
import org.apache.iotdb.udf.api.UDFResultSet;
import org.apache.iotdb.udf.api.exception.UDFException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/** Server-side implementation of {@link IoTDBLocal}. */
public class IoTDBLocalImpl implements IoTDBLocal {

  public static final IoTDBLocalFactory FACTORY = IoTDBLocalImpl::new;

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBLocalImpl.class);

  private final SessionInfo sessionInfo;
  private final String fragmentInstanceId;
  private final QueryId outerGlobalQueryId;
  private final long outerQueryDeadlineMs;
  private final List<UDFResultSetImpl> openResultSets = new ArrayList<>();

  public IoTDBLocalImpl(
      SessionInfo sessionInfo,
      String fragmentInstanceId,
      String outerGlobalQueryId,
      long outerQueryDeadlineMs) {
    this.sessionInfo = sessionInfo;
    this.fragmentInstanceId = fragmentInstanceId;
    this.outerGlobalQueryId = QueryId.valueOf(outerGlobalQueryId);
    this.outerQueryDeadlineMs = outerQueryDeadlineMs;
  }

  @Override
  public UDFResultSet query(String sql) throws UDFException {
    try {
      long timeoutMs = computeRemainingTimeoutMs();
      if (timeoutMs <= 0) {
        throw new QueryTimeoutException(
            DataNodeQueryMessages
                .EXCEPTION_OUTER_QUERY_TIMEOUT_EXCEEDED_BEFORE_IOTDBLOCAL_QUERY_STARTS_800BFA63);
      }
      InternalQueryResult result =
          InternalQueryExecutor.executeInternalQuery(
              sessionInfo, fragmentInstanceId, outerGlobalQueryId, sql, timeoutMs);
      int index = openResultSets.size();
      UDFResultSetImpl rs = new UDFResultSetImpl(openResultSets, index, result);
      openResultSets.add(rs);
      return rs;
    } catch (IoTDBException e) {
      throw new UDFException(e.getMessage(), e);
    }
  }

  @Override
  public void close() {
    for (int i = 0; i < openResultSets.size(); i++) {
      UDFResultSetImpl rs = openResultSets.get(i);
      if (rs != null) {
        try {
          rs.close();
        } catch (UDFException e) {
          LOGGER.warn(
              DataNodeQueryMessages.MESSAGE_FAILED_TO_CLOSE_UDF_RESULT_SET_AT_INDEX_ARG_A293B7EC,
              i,
              e);
        }
      }
    }
    openResultSets.clear();
  }

  private long computeRemainingTimeoutMs() {
    if (outerQueryDeadlineMs <= 0) {
      return 0;
    }
    return outerQueryDeadlineMs - System.currentTimeMillis();
  }

  @Override
  public void info(String msg) {
    LOGGER.info(msg);
  }

  @Override
  public void info(String format, Object... args) {
    LOGGER.info(format, args);
  }

  @Override
  public void info(String msg, Throwable t) {
    LOGGER.info(msg, t);
  }

  @Override
  public void warn(String msg) {
    LOGGER.warn(msg);
  }

  @Override
  public void warn(String format, Object... args) {
    LOGGER.warn(format, args);
  }

  @Override
  public void warn(String msg, Throwable t) {
    LOGGER.warn(msg, t);
  }

  @Override
  public void error(String msg) {
    LOGGER.error(msg);
  }

  @Override
  public void error(String format, Object... args) {
    LOGGER.error(format, args);
  }

  @Override
  public void error(String msg, Throwable t) {
    LOGGER.error(msg, t);
  }
}
