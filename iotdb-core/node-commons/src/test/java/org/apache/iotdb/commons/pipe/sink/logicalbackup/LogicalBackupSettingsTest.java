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

package org.apache.iotdb.commons.pipe.sink.logicalbackup;

import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class LogicalBackupSettingsTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testInvalidSettingsFailBeforeCreatingDirectory() throws Exception {
    final Path root = temporaryFolder.getRoot().toPath().resolve("backup");
    for (final Map<String, String> invalid :
        Arrays.asList(
            Map.of("sink.backup-id", "../outside"),
            Map.of("sink.backup-id", "bad\\path"),
            Map.of("sink.backup-id", "CON"),
            Map.of("sink.segment-size-bytes", "1024"),
            Map.of("sink.max-record-bytes", "268435457"),
            Map.of("sink.fsync-policy", "batch", "sink.fsync-batch-operations", "0"),
            Map.of("sink.fsync-period-ms", "1000"),
            Map.of("sink.include-heartbeat", "maybe"),
            Map.of("sink.on-unsupported-event", "ignore"))) {
      final Map<String, String> values = new HashMap<>(invalid);
      values.put("sink.dir", root.toString());
      try {
        new LogicalBackupSettings(new PipeParameters(values), "backup");
        Assert.fail(invalid.toString());
      } catch (final IOException expected) {
        Assert.assertFalse(Files.exists(root));
      }
    }
  }

  @Test
  public void testAllStreamsUseTheSameWriterSettingsAndAppendIdentity() throws Exception {
    final Path root = temporaryFolder.getRoot().toPath().resolve("backup");
    final Map<String, String> values = new HashMap<>();
    values.put("sink.dir", root.toString());
    values.put("sink.resume", "append");
    values.put("sink.backup-id", "backup-id");
    values.put("sink.segment-size-bytes", "8192");
    values.put("sink.max-record-bytes", "1024");
    values.put("sink.fsync-policy", "periodic");
    values.put("sink.fsync-period-ms", "12");
    values.put("sink.capture-options", "{\"scope\":\"config,schema,data\"}");
    final LogicalBackupSettings settings =
        new LogicalBackupSettings(new PipeParameters(values), "backup");
    for (final String type : Arrays.asList("config", "schema", "data")) {
      final LogicalBackupManifest manifest = new LogicalBackupManifest();
      manifest.backupId = "backup-id";
      manifest.streamId = type;
      manifest.streamType = type;
      manifest.timestampPrecision = "ms";
      try (final LogicalBackupWriter writer = settings.open(type, 1, manifest)) {
        Assert.assertEquals(8192, writer.getManifest().segmentSizeBytes);
        Assert.assertEquals(1024, writer.getManifest().maxRecordBytes);
        Assert.assertEquals("periodic", writer.getManifest().fsyncPolicy);
      }
      values.put("sink.capture-options", "{\"scope\":\"data\"}");
      try {
        new LogicalBackupSettings(new PipeParameters(values), "backup").open(type, 1, manifest);
        Assert.fail();
      } catch (final IOException expected) {
        // A different scope cannot silently append to an existing stream.
      }
    }
  }
}
