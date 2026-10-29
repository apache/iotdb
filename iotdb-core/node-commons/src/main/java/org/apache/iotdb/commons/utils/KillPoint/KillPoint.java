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

package org.apache.iotdb.commons.utils.KillPoint;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.i18n.UtilMessages;
import org.apache.iotdb.commons.utils.TestOnly;

import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

public class KillPoint {
  private static final Logger LOGGER = LoggerFactory.getLogger(KillPoint.class);

  /** Set only by the integration-test node launcher; gates belong to one test-owned node. */
  public static final String TEST_GATE_DIR = "iotdb.test.killpoint.dir";

  public static String addKillPointPrefix(String name) {
    return "[KILL POINT] " + name;
  }

  /**
   * @param s something like "[a, b, c]"
   * @return Set{a,b,c}
   */
  public static ImmutableSet<String> parseKillPoints(String s) {
    if (s == null) {
      return ImmutableSet.of();
    }
    Set<String> result =
        Arrays.stream(s.replace("[", "").replace("]", "").replace(" ", "").split(","))
            .collect(Collectors.toSet());
    LOGGER.info(UtilMessages.KILL_POINT_SET, result);
    return ImmutableSet.copyOf(result);
  }

  /**
   * Call this function to print a line in log.
   *
   * @param x Any enum member
   */
  public static <T extends Enum<T>> void setKillPoint(T x) {
    if (CommonDescriptor.getInstance().getConfig().isIntegrationTest()) {
      String breakPointName = enumToString(x);
      testOnlyVisitGate(breakPointName);
      if (CommonDescriptor.getInstance()
          .getConfig()
          .getEnabledKillPoints()
          .contains(breakPointName)) {
        LOGGER.info(addKillPointPrefix(breakPointName));
        try {
          TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  @TestOnly
  private static void testOnlyVisitGate(String name) {
    final String directory = System.getProperty(TEST_GATE_DIR);
    if (directory == null) {
      return;
    }
    final Path pause = Paths.get(directory, name + ".pause");
    final Path watch = Paths.get(directory, name + ".watch");
    if (!Files.exists(pause) && !Files.exists(watch)) {
      return;
    }
    try {
      Files.write(Paths.get(directory, name + ".reached"), new byte[0]);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    // A bounded, explicitly armed barrier lets ITs observe a real state boundary, perform a
    // failure or a concurrent operation, and release it. Never wait indefinitely after a failed IT.
    final long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(2);
    while (Files.exists(pause)
        && !Thread.currentThread().isInterrupted()
        && System.nanoTime() < deadline) {
      LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
    }
  }

  public static <T extends Enum<?>> String enumToString(T x) {
    return x.getClass().getSimpleName() + "." + x.name();
  }
}
