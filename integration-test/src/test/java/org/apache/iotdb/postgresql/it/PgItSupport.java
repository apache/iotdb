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

package org.apache.iotdb.postgresql.it;

import org.apache.iotdb.it.env.EnvFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Helpers for the PostgreSQL wire protocol ITs . */
final class PgItSupport {

  private static final long WAIT_NANOS = java.util.concurrent.TimeUnit.SECONDS.toNanos(60);

  private PgItSupport() {}

  /** The DataNode's log output so far. */
  static String dataNodeLog() {
    Path dir = Paths.get(EnvFactory.getEnv().getDataNodeWrapperList().get(0).getLogDirPath());
    try (Stream<Path> files = Files.list(dir)) {
      return files
          .filter(p -> p.getFileName().toString().startsWith("DataNode"))
          .map(
              p -> {
                try {
                  return new String(Files.readAllBytes(p), StandardCharsets.UTF_8);
                } catch (IOException e) {
                  throw new UncheckedIOException(e);
                }
              })
          .collect(Collectors.joining("\n"));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Waits until the DataNode's log satisfies {@code condition}: the service starts after the
   * DataNode reports itself ready, and slow machines write the log later.
   */
  static String awaitDataNodeLog(Predicate<String> condition) throws InterruptedException {
    long deadline = System.nanoTime() + WAIT_NANOS;
    String log = dataNodeLog();
    while (!condition.test(log) && System.nanoTime() < deadline) {
      Thread.sleep(200);
      log = dataNodeLog();
    }
    return log;
  }

  /**
   * Waits until the PostgreSQL endpoint accepts connections: the service starts after the DataNode
   * reports itself ready.
   */
  static void awaitListening(int port) throws InterruptedException {
    long deadline = System.nanoTime() + WAIT_NANOS;
    while (!listening(port)) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError("the PostgreSQL endpoint does not listen on port " + port);
      }
      Thread.sleep(200);
    }
  }

  /** True if something accepts connections on 127.0.0.1:port. */
  static boolean listening(int port) {
    try (java.net.Socket socket = new java.net.Socket()) {
      socket.connect(new java.net.InetSocketAddress("127.0.0.1", port), 1000);
      return true;
    } catch (IOException e) {
      return false;
    }
  }
}
