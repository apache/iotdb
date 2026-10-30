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

package org.apache.iotdb.it.fault.metadata;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Controls called only by the metadata IT agent in child node JVMs. This class is packaged into a
 * temporary test agent, never a server artifact. Controls live beneath the child node's IOTDB_HOME;
 * removing a control releases the captured call without restarting it.
 */
public final class MetadataTestHooks {
  public static final String DIRECTORY = "metadata-test-hooks";
  public static final String CONFIG_NODE_RPC = "config-node-rpc";
  public static final String LEASE_RECOVERY = "lease-recovery";
  public static final String ATTRIBUTE_UPDATE = "attribute-update";
  public static final String SNAPSHOT_CREATED = "snapshot-created";
  public static final String SNAPSHOT_LOADED = "snapshot-loaded";

  private MetadataTestHooks() {}

  private static Path directory() {
    return Paths.get(System.getProperty("IOTDB_HOME"), DIRECTORY);
  }

  public static boolean fail(final String point) {
    if (Files.exists(directory().resolve("fail." + point))) {
      record(point, "failed");
      record("failure." + point, "failed");
      return true;
    }
    return false;
  }

  public static void pause(final String point, final String context) throws InterruptedException {
    final Path control = directory().resolve("pause." + point);
    if (Files.exists(control)) {
      final Path blocked = directory().resolve("blocked." + point);
      try {
        Files.move(control, blocked);
        record(point, context);
        while (Files.exists(blocked)) {
          TimeUnit.MILLISECONDS.sleep(10);
        }
      } catch (NoSuchFileException | FileAlreadyExistsException ignored) {
        // Only the captured invocation waits. Later retries can overtake this old request.
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  public static void observe(final String point, final String context) {
    if (Files.exists(directory().resolve("observe." + point))) {
      record(point, context);
    }
  }

  public static void observe(final String point, final String invocation, final String context) {
    if (Files.exists(directory().resolve("observe." + point))) {
      record(point + "." + invocation, context);
    }
  }

  private static void record(final String point, final String context) {
    try {
      // Keep the first invocation, including its procedure ID / snapshot path, as evidence.
      Files.write(
          directory().resolve("hit." + point),
          context.getBytes(StandardCharsets.UTF_8),
          StandardOpenOption.CREATE_NEW,
          StandardOpenOption.WRITE);
    } catch (FileAlreadyExistsException ignored) {
      // A retry must not replace the evidence for the original invocation.
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static boolean procedure(final Object procedure, final Object state)
      throws InterruptedException {
    if (!procedure
        .getClass()
        .getName()
        .startsWith("org.apache.iotdb.confignode.procedure.impl.schema.")) {
      return false;
    }
    final String point = "procedure." + procedure.getClass().getSimpleName() + "." + state;
    try {
      pause(point, procedure.getClass().getMethod("getProcId").invoke(procedure).toString());
    } catch (ReflectiveOperationException e) {
      throw new AssertionError("Cannot observe the real procedure ID", e);
    }
    return fail(point);
  }

  public static String captureAttributeUpdate() {
    final String invocation = UUID.randomUUID().toString();
    try {
      pause(ATTRIBUTE_UPDATE, invocation);
      return invocation;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return null;
    }
  }

  static void agentStarted() throws IOException {
    Files.createDirectories(directory());
    record("agent-ready", "ready");
  }

  static void transformed(final String name) {
    record("instrumented." + name, "ready");
  }

  static void transformationFailed(final String name, final Exception failure) {
    record("instrumentation-error", name + ": " + failure);
  }
}
