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

package org.apache.iotdb.db.consensus.statemachine.schemaregion;

import org.apache.iotdb.commons.exception.MetadataException;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

public class SchemaExecutionVisitorTest {
  @Test
  public void testAuthLogger() {
    SchemaExecutionVisitor.setLogger(
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
            return true;
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
            return true;
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
          public void warn(String msg) {}

          @Override
          public void warn(String format, Object arg) {}

          @Override
          public void warn(String format, Object... arguments) {}

          @Override
          public void warn(String format, Object arg1, Object arg2) {}

          @Override
          public void warn(String msg, Throwable t) {}

          @Override
          public boolean isWarnEnabled(Marker marker) {
            return false;
          }

          @Override
          public void warn(Marker marker, String msg) {}

          @Override
          public void warn(Marker marker, String format, Object arg) {}

          @Override
          public void warn(Marker marker, String format, Object arg1, Object arg2) {}

          @Override
          public void warn(Marker marker, String format, Object... arguments) {}

          @Override
          public void warn(Marker marker, String msg, Throwable t) {}

          @Override
          public boolean isErrorEnabled() {
            return true;
          }

          @Override
          public void error(String msg) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void error(String format, Object arg) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void error(String format, Object arg1, Object arg2) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void error(String format, Object... arguments) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void error(String msg, Throwable t) {
            throw new UnsupportedOperationException();
          }

          @Override
          public boolean isErrorEnabled(Marker marker) {
            return true;
          }

          @Override
          public void error(Marker marker, String msg) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void error(Marker marker, String format, Object arg) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void error(Marker marker, String format, Object arg1, Object arg2) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void error(Marker marker, String format, Object... arguments) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void error(Marker marker, String msg, Throwable t) {
            throw new UnsupportedOperationException();
          }
        });
    SchemaExecutionVisitor.logMetaDataException(new MetadataException("", true));
    SchemaExecutionVisitor.logMetaDataException("", new MetadataException("", true));
    try {
      SchemaExecutionVisitor.logMetaDataException(new MetadataException("", false));
      Assert.fail();
    } catch (final UnsupportedOperationException e) {
      // Expected
    }
    try {
      SchemaExecutionVisitor.logMetaDataException(new MetadataException("", false));
      Assert.fail();
    } catch (final UnsupportedOperationException e) {
      // Expected
    }
    SchemaExecutionVisitor.setLogger(LoggerFactory.getLogger(SchemaExecutionVisitor.class));
  }
}
