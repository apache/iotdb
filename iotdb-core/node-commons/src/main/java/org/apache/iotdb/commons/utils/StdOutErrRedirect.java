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

package org.apache.iotdb.commons.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.io.PrintStream;

public class StdOutErrRedirect {
  private static final Logger LOGGER = LoggerFactory.getLogger(StdOutErrRedirect.class);

  private StdOutErrRedirect() {}

  public static void redirectSystemOutAndErrToLog() {
    System.setOut(new PrintStream(new InfoLoggerStream(LOGGER)));
    System.setErr(new PrintStream(new ErrorLoggerStream(LOGGER)));
  }

  private static class InfoLoggerStream extends OutputStream {

    private final Logger logger;

    public InfoLoggerStream(Logger logger) {
      super();
      this.logger = logger;
    }

    @Override
    public void write(int b) {
      String string = String.valueOf((char) b);
      if (!string.trim().isEmpty()) logger.info(string);
    }

    @Override
    public void write(byte[] b) {
      String string = new String(b);
      if (!string.trim().isEmpty()) {
        logger.info(string);
      }
    }

    @Override
    public void write(byte[] b, int off, int len) {
      String string = new String(b, off, len);
      if (!string.trim().isEmpty()) logger.info(string);
    }
  }

  private static class ErrorLoggerStream extends OutputStream {

    private final Logger logger;

    public ErrorLoggerStream(Logger logger) {
      super();
      this.logger = logger;
    }

    @Override
    public void write(int b) {
      String string = String.valueOf((char) b);
      if (!string.trim().isEmpty()) logger.error(string);
    }

    @Override
    public void write(byte[] b) {
      String string = new String(b);
      if (!string.trim().isEmpty()) {
        logger.error(string);
      }
    }

    @Override
    public void write(byte[] b, int off, int len) {
      String string = new String(b, off, len);
      if (!string.trim().isEmpty()) logger.error(string);
    }
  }
}
