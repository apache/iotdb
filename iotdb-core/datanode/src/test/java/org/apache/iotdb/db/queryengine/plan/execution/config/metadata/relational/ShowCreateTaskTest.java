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

package org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant;
import org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant;
import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseInfo;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ShowCreateTaskTest {

  @Test
  public void testShowCreateDatabaseSQL() {
    final TDatabaseInfo databaseInfo = new TDatabaseInfo();
    databaseInfo.setName("test_db");
    databaseInfo.setTTL(Long.MAX_VALUE);
    databaseInfo.setTimePartitionInterval(604800000L);
    databaseInfo.setMinSchemaRegionNum(0);
    databaseInfo.setMinDataRegionNum(1);

    assertEquals(
        "CREATE DATABASE \"test_db\" WITH (ttl='"
            + IoTDBConstant.TTL_INFINITE
            + "',time_partition_interval=604800000,schema_region_group_num=0,data_region_group_num=1)",
        ShowCreateDatabaseTask.getShowCreateDatabaseSQL(databaseInfo));
  }

  @Test
  public void testShowCreatePipeSQLShouldSanitizeInternalAndInjectedAttributes() {
    final Map<String, String> sourceAttributes = new HashMap<>();
    sourceAttributes.put(PipeSourceConstant.SOURCE_KEY, "iotdb-source");
    sourceAttributes.put(SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TABLE_VALUE);
    sourceAttributes.put("__audit.source", "audit");
    sourceAttributes.put(PipeSourceConstant.SOURCE_IOTDB_USER_ID, "1");
    sourceAttributes.put(PipeSourceConstant.SOURCE_IOTDB_USERNAME_KEY, "alice");
    sourceAttributes.put(PipeSourceConstant.SOURCE_IOTDB_CLI_HOSTNAME, "host");

    final Map<String, String> processorAttributes = new HashMap<>();
    processorAttributes.put(PipeProcessorConstant.PROCESSOR_KEY, "do-nothing-processor");
    processorAttributes.put(SystemConstant.RESTART_OR_NEWLY_ADDED_KEY, "true");
    processorAttributes.put("__audit.processor", "audit");

    final Map<String, String> sinkAttributes = new HashMap<>();
    sinkAttributes.put(PipeSinkConstant.SINK_KEY, "write-back-sink");
    sinkAttributes.put(SystemConstant.RESTART_OR_NEWLY_ADDED_KEY, "true");
    sinkAttributes.put("__audit.sink", "audit");
    sinkAttributes.put(PipeSinkConstant.SINK_IOTDB_USER_ID, "1");
    sinkAttributes.put(PipeSinkConstant.SINK_IOTDB_USERNAME_KEY, "alice");
    sinkAttributes.put(PipeSinkConstant.SINK_IOTDB_CLI_HOSTNAME, "host");

    final PipeMeta pipeMeta =
        new PipeMeta(
            new PipeStaticMeta(
                "test_pipe", 1L, sourceAttributes, processorAttributes, sinkAttributes),
            new PipeRuntimeMeta());

    assertEquals(
        "CREATE PIPE \"test_pipe\" WITH SOURCE ('source'='iotdb-source')"
            + " WITH PROCESSOR ('processor'='do-nothing-processor')"
            + " WITH SINK ('sink'='write-back-sink')",
        ShowCreatePipeTask.getShowCreatePipeSQL(pipeMeta));
  }

  @Test
  public void testShowCreatePipeSQLShouldKeepExplicitCredentials() {
    final Map<String, String> sourceAttributes = new HashMap<>();
    sourceAttributes.put(PipeSourceConstant.SOURCE_KEY, "iotdb-source");
    sourceAttributes.put(PipeSourceConstant.SOURCE_IOTDB_USERNAME_KEY, "alice");
    sourceAttributes.put(PipeSourceConstant.SOURCE_IOTDB_PASSWORD_KEY, "secret");
    sourceAttributes.put(PipeSourceConstant.SOURCE_IOTDB_USER_ID, "1");
    sourceAttributes.put(PipeSourceConstant.SOURCE_IOTDB_CLI_HOSTNAME, "host");

    final Map<String, String> sinkAttributes = new HashMap<>();
    sinkAttributes.put(PipeSinkConstant.SINK_KEY, "write-back-sink");
    sinkAttributes.put(PipeSinkConstant.SINK_IOTDB_USERNAME_KEY, "alice");
    sinkAttributes.put(PipeSinkConstant.SINK_IOTDB_PASSWORD_KEY, "secret");
    sinkAttributes.put(PipeSinkConstant.SINK_IOTDB_USER_ID, "1");
    sinkAttributes.put(PipeSinkConstant.SINK_IOTDB_CLI_HOSTNAME, "host");

    final PipeMeta pipeMeta =
        new PipeMeta(
            new PipeStaticMeta("test_pipe", 1L, sourceAttributes, new HashMap<>(), sinkAttributes),
            new PipeRuntimeMeta());

    assertEquals(
        "CREATE PIPE \"test_pipe\""
            + " WITH SOURCE ('source'='iotdb-source','source.password'='secret','source.username'='alice')"
            + " WITH SINK ('sink'='write-back-sink','sink.password'='secret','sink.username'='alice')",
        ShowCreatePipeTask.getShowCreatePipeSQL(pipeMeta));
  }

  @Test
  public void testShowCreatePipeSQLShouldSanitizeExtractorAndConnectorAliases() {
    final Map<String, String> sourceAttributes = new HashMap<>();
    sourceAttributes.put(PipeSourceConstant.EXTRACTOR_KEY, "iotdb-extractor");
    sourceAttributes.put(PipeSourceConstant.EXTRACTOR_IOTDB_USER_ID, "1");
    sourceAttributes.put(PipeSourceConstant.EXTRACTOR_IOTDB_USERNAME_KEY, "alice");
    sourceAttributes.put(PipeSourceConstant.EXTRACTOR_IOTDB_CLI_HOSTNAME, "host");

    final Map<String, String> sinkAttributes = new HashMap<>();
    sinkAttributes.put(PipeSinkConstant.CONNECTOR_KEY, "iotdb-thrift-connector");
    sinkAttributes.put(PipeSinkConstant.CONNECTOR_IOTDB_USER_ID, "1");
    sinkAttributes.put(PipeSinkConstant.CONNECTOR_IOTDB_USERNAME_KEY, "alice");
    sinkAttributes.put(PipeSinkConstant.CONNECTOR_IOTDB_CLI_HOSTNAME, "host");

    final PipeMeta pipeMeta =
        new PipeMeta(
            new PipeStaticMeta("test_pipe", 1L, sourceAttributes, new HashMap<>(), sinkAttributes),
            new PipeRuntimeMeta());

    assertEquals(
        "CREATE PIPE \"test_pipe\""
            + " WITH SOURCE ('extractor'='iotdb-extractor')"
            + " WITH SINK ('connector'='iotdb-thrift-connector')",
        ShowCreatePipeTask.getShowCreatePipeSQL(pipeMeta));
  }
}
