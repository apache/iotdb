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

package org.apache.iotdb.db.pipe.sink.protocol.logicalbackup;

import org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.exception.PipeParameterNotValidException;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class LogicalBackupSinkTest {

  @Test
  public void testDataRegionRequiresExplicitAppendResumePolicy() throws Exception {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(PipeSinkConstant.SINK_LOGICAL_BACKUP_DIR_KEY, "backup");

    assertInvalid(new LogicalBackupSink("data"), attributes);

    attributes.put(
        PipeSinkConstant.SINK_LOGICAL_BACKUP_RESUME_KEY,
        PipeSinkConstant.LOGICAL_BACKUP_RESUME_FAIL_IF_EXISTS);
    assertInvalid(new LogicalBackupSink("data"), attributes);

    attributes.put(
        PipeSinkConstant.SINK_LOGICAL_BACKUP_RESUME_KEY,
        PipeSinkConstant.LOGICAL_BACKUP_RESUME_APPEND);
    new LogicalBackupSink("data")
        .validate(new PipeParameterValidator(new PipeParameters(attributes)));

    attributes.remove(PipeSinkConstant.SINK_LOGICAL_BACKUP_RESUME_KEY);
    attributes.put(
        PipeSinkConstant.CONNECTOR_LOGICAL_BACKUP_RESUME_KEY,
        PipeSinkConstant.LOGICAL_BACKUP_RESUME_APPEND);
    new LogicalBackupSink("data")
        .validate(new PipeParameterValidator(new PipeParameters(attributes)));
  }

  @Test
  public void testSchemaRegionKeepsDefaultResumePolicy() throws Exception {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(PipeSinkConstant.SINK_LOGICAL_BACKUP_DIR_KEY, "backup");

    new LogicalBackupSink("schema")
        .validate(new PipeParameterValidator(new PipeParameters(attributes)));
  }

  private static void assertInvalid(
      final LogicalBackupSink sink, final Map<String, String> attributes) throws Exception {
    try {
      sink.validate(new PipeParameterValidator(new PipeParameters(new HashMap<>(attributes))));
      Assert.fail();
    } catch (final PipeParameterNotValidException expected) {
      Assert.assertTrue(expected.getMessage().contains("append"));
    }
  }
}
