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

package org.apache.iotdb.commons.pipe.resource;

import org.apache.iotdb.commons.pipe.resource.log.PipeLogger;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

public class PipeLoggerTest {

  @Test
  public void testLogMessageWithPercent() {
    final AtomicReference<String> message = new AtomicReference<>();

    setStringFormatLogger();
    try {
      PipeLogger.log(message::set, "data_{sink.password=%/broken}");
    } finally {
      setStringFormatLogger();
    }

    Assert.assertEquals("data_{sink.password=%/broken}", message.get());
  }

  @Test
  public void testLogMessageWithSlf4jPlaceholder() {
    final AtomicReference<String> message = new AtomicReference<>();

    setStringFormatLogger();
    try {
      PipeLogger.log(message::set, "PipeLoggerCacheMaxSizeInBytes: {}", 1024);
    } finally {
      setStringFormatLogger();
    }

    Assert.assertEquals("PipeLoggerCacheMaxSizeInBytes: 1024", message.get());
  }

  @Test
  public void testLogThrowableWithPercentInStackTrace() {
    final AtomicReference<String> message = new AtomicReference<>();

    setStringFormatLogger();
    try {
      PipeLogger.log(
          message::set,
          new RuntimeException("data_{sink.password=%/broken}"),
          "Failed to transfer event %s",
          "root.sg.d1");
    } finally {
      setStringFormatLogger();
    }

    Assert.assertTrue(message.get().contains("Failed to transfer event root.sg.d1"));
    Assert.assertTrue(message.get().contains("data_{sink.password=%/broken}"));
  }

  private void setStringFormatLogger() {
    PipeLogger.setLogger(
        (loggerFunction, rawMessage, formatter) ->
            loggerFunction.accept(String.format(rawMessage, formatter)));
  }
}
