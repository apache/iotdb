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

package org.apache.iotdb.commons.pipe.resource.log;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.function.Consumer;

public class PipeLogger {
  private static PipePeriodicalLogger logger =
      (loggerFunction, rawMessage, formatter) ->
          loggerFunction.accept(String.format(rawMessage, formatter));

  public static void log(
      final Consumer<String> loggerFunction, final String rawMessage, final Object... formatter) {
    logger.log(loggerFunction, rawMessage, formatter);
  }

  public static void log(
      final Consumer<String> loggerFunction,
      final Throwable throwable,
      final String rawMessage,
      final Object... formatter) {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    throwable.printStackTrace(new PrintStream(out));
    logger.log(loggerFunction, rawMessage + "\n" + out, formatter);
  }

  public static void setLogger(final PipePeriodicalLogger logger) {
    PipeLogger.logger = logger;
  }

  private PipeLogger() {
    // static
  }

  @FunctionalInterface
  public interface PipePeriodicalLogger {
    void log(final Consumer<String> loggerFunction, final String rawMessage, final Object... args);
  }
}
