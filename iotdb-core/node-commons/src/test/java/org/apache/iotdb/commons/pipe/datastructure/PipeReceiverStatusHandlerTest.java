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

package org.apache.iotdb.commons.pipe.datastructure;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.pipe.receiver.PipeReceiverStatusHandler;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_EXCEPTION_CONFLICT_RECORD_IGNORED_DATA_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_EXCEPTION_CONFLICT_RESOLVE_STRATEGY_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_EXCEPTION_CONFLICT_RETRY_MAX_TIME_SECONDS_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_EXCEPTION_OTHERS_RECORD_IGNORED_DATA_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_EXCEPTION_OTHERS_RETRY_MAX_TIME_SECONDS_DEFAULT_VALUE;

public class PipeReceiverStatusHandlerTest {
  @Test
  public void testAuthLogger() {
    final PipeReceiverStatusHandler handler =
        new PipeReceiverStatusHandler(
            CONNECTOR_EXCEPTION_CONFLICT_RESOLVE_STRATEGY_DEFAULT_VALUE.equals("retry"),
            CONNECTOR_EXCEPTION_CONFLICT_RETRY_MAX_TIME_SECONDS_DEFAULT_VALUE,
            CONNECTOR_EXCEPTION_CONFLICT_RECORD_IGNORED_DATA_DEFAULT_VALUE,
            CONNECTOR_EXCEPTION_OTHERS_RETRY_MAX_TIME_SECONDS_DEFAULT_VALUE,
            CONNECTOR_EXCEPTION_OTHERS_RECORD_IGNORED_DATA_DEFAULT_VALUE,
            true);
    PipeReceiverStatusHandler.setLogger(
        new Logger() {
          @Override
          public String getName() {
            return null;
          }

          @Override
          public boolean isTraceEnabled() {
            return false;
          }

          @Override
          public void trace(String msg) {}

          @Override
          public void trace(String format, Object arg) {}

          @Override
          public void trace(String format, Object arg1, Object arg2) {}

          @Override
          public void trace(String format, Object... arguments) {}

          @Override
          public void trace(String msg, Throwable t) {}

          @Override
          public boolean isTraceEnabled(Marker marker) {
            return false;
          }

          @Override
          public void trace(Marker marker, String msg) {}

          @Override
          public void trace(Marker marker, String format, Object arg) {}

          @Override
          public void trace(Marker marker, String format, Object arg1, Object arg2) {}

          @Override
          public void trace(Marker marker, String format, Object... argArray) {}

          @Override
          public void trace(Marker marker, String msg, Throwable t) {}

          @Override
          public boolean isDebugEnabled() {
            return false;
          }

          @Override
          public void debug(String msg) {}

          @Override
          public void debug(String format, Object arg) {}

          @Override
          public void debug(String format, Object arg1, Object arg2) {}

          @Override
          public void debug(String format, Object... arguments) {}

          @Override
          public void debug(String msg, Throwable t) {}

          @Override
          public boolean isDebugEnabled(Marker marker) {
            return false;
          }

          @Override
          public void debug(Marker marker, String msg) {}

          @Override
          public void debug(Marker marker, String format, Object arg) {}

          @Override
          public void debug(Marker marker, String format, Object arg1, Object arg2) {}

          @Override
          public void debug(Marker marker, String format, Object... arguments) {}

          @Override
          public void debug(Marker marker, String msg, Throwable t) {}

          @Override
          public boolean isInfoEnabled() {
            return false;
          }

          @Override
          public void info(String msg) {}

          @Override
          public void info(String format, Object arg) {}

          @Override
          public void info(String format, Object arg1, Object arg2) {}

          @Override
          public void info(String format, Object... arguments) {}

          @Override
          public void info(String msg, Throwable t) {}

          @Override
          public boolean isInfoEnabled(Marker marker) {
            return false;
          }

          @Override
          public void info(Marker marker, String msg) {}

          @Override
          public void info(Marker marker, String format, Object arg) {}

          @Override
          public void info(Marker marker, String format, Object arg1, Object arg2) {}

          @Override
          public void info(Marker marker, String format, Object... arguments) {}

          @Override
          public void info(Marker marker, String msg, Throwable t) {}

          // Warn
          @Override
          public boolean isWarnEnabled() {
            return true;
          }

          @Override
          public void warn(String msg) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void warn(String format, Object arg) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void warn(String format, Object... arguments) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void warn(String format, Object arg1, Object arg2) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void warn(String msg, Throwable t) {
            throw new UnsupportedOperationException();
          }

          @Override
          public boolean isWarnEnabled(Marker marker) {
            return true;
          }

          @Override
          public void warn(Marker marker, String msg) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void warn(Marker marker, String format, Object arg) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void warn(Marker marker, String format, Object arg1, Object arg2) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void warn(Marker marker, String format, Object... arguments) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void warn(Marker marker, String msg, Throwable t) {
            throw new UnsupportedOperationException();
          }

          @Override
          public boolean isErrorEnabled() {
            return false;
          }

          @Override
          public void error(String msg) {}

          @Override
          public void error(String format, Object arg) {}

          @Override
          public void error(String format, Object arg1, Object arg2) {}

          @Override
          public void error(String format, Object... arguments) {}

          @Override
          public void error(String msg, Throwable t) {}

          @Override
          public boolean isErrorEnabled(Marker marker) {
            return false;
          }

          @Override
          public void error(Marker marker, String msg) {}

          @Override
          public void error(Marker marker, String format, Object arg) {}

          @Override
          public void error(Marker marker, String format, Object arg1, Object arg2) {}

          @Override
          public void error(Marker marker, String format, Object... arguments) {}

          @Override
          public void error(Marker marker, String msg, Throwable t) {}
        });
    handler.handle(
        new TSStatus(TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode()),
        "",
        "");
    handler.handle(new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode()), "", "");
    try {
      handler.handle(new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode()), "", "", true);
      Assert.fail();
    } catch (final UnsupportedOperationException e) {
      // Expected
    }
    handler.handle(new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode()), "", "");
    try {
      handler.handle(
          new TSStatus(TSStatusCode.METADATA_ERROR.getStatusCode())
              .setMessage("No permissions for this operation, please add privilege WRITE_DATA"),
          "",
          "",
          true);
      Assert.fail();
    } catch (final UnsupportedOperationException e) {
      // Expected
    }
    PipeReceiverStatusHandler.setLogger(LoggerFactory.getLogger(PipeReceiverStatusHandler.class));
  }
}
