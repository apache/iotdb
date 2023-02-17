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

package org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.log;

import org.apache.iotdb.db.metadata.logfile.SchemaLogWriter;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.ISchemaPage;
import org.apache.iotdb.db.metadata.mtree.store.disk.schemafile.SchemaFileConfig;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A specialisation of {@linkplain SchemaLogWriter} for integrity of SchemaFile, acting like a redo
 * log as in InnoDB.
 */
public class SchemaFileLogWriter extends SchemaLogWriter<ISchemaPage> {

  // TODO: improve if the following considered as waste
  public static final ISchemaPage PREPARE_MARK =
      ISchemaPage.initSegmentedPage(
          ByteBuffer.allocate(SchemaFileConfig.PAGE_HEADER_SIZE), SchemaFileConfig.SF_PREPARE_MARK);
  public static final ISchemaPage COMMIT_MARK =
      ISchemaPage.initSegmentedPage(
          ByteBuffer.allocate(SchemaFileConfig.PAGE_HEADER_SIZE), SchemaFileConfig.SF_COMMIT_MARK);
  public static final SchemaFileLogSerializer SERIALIZER = new SchemaFileLogSerializer();

  private final String logPath;

  public SchemaFileLogWriter(String logFilePath) throws IOException {
    super(logFilePath, SERIALIZER, false);
    logPath = logFilePath;
  }

  public synchronized void prepare() throws IOException {
    // mark that all dirty pages had been logged during a flush
    write(PREPARE_MARK);
    force();
  }

  public synchronized void commit() throws IOException {
    // all dirty pages had been flushed correctly, entries before this is disposable now
    write(COMMIT_MARK);
    force();
  }

  /** Since SchemaFileLog imitates a redo log, renewal is needed periodically. */
  public SchemaFileLogWriter renew() throws IOException {
    clear();
    return new SchemaFileLogWriter(logPath);
  }
}
