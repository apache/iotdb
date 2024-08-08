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

package org.apache.iotdb.flink.tsfile;

import org.apache.iotdb.flink.tsfile.util.TSFileConfigUtil;
import org.apache.iotdb.hadoop.fileSystem.HDFSOutput;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.schema.Schema;
import org.apache.iotdb.tsfile.write.writer.LocalTsFileOutput;
import org.apache.iotdb.tsfile.write.writer.TsFileOutput;

import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.util.HadoopUtils;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.conf.Configuration;

import javax.annotation.Nullable;

import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;

/**
 * The abstract base class of the output formats which write data to TsFile.
 *
 * @param <T> The input data type.
 */
public abstract class TsFileOutputFormat<T> extends FileOutputFormat<T> {

  protected Schema schema;
  @Nullable protected TSFileConfig config;

  protected transient Configuration hadoopConf = null;
  private FileOutputStream fos = null;
  protected transient TsFileWriter writer = null;

  public TsFileOutputFormat(String path, Schema schema, TSFileConfig config) {
    super(path == null ? null : new Path(path));
    this.schema = Preconditions.checkNotNull(schema);
    this.config = config;
  }

  @Override
  public void configure(org.apache.flink.configuration.Configuration flinkConfiguration) {
    super.configure(flinkConfiguration);
    hadoopConf = HadoopUtils.getHadoopConfiguration(flinkConfiguration);
  }

  @Override
  public void open(int taskNumber, int numTasks) throws IOException {
    super.open(taskNumber, numTasks);
    if (config != null) {
      TSFileConfigUtil.setGlobalTSFileConfig(config);
    }
    // Use TsFile API to write instead of FSDataOutputStream.
    this.stream.close();
    Path actualFilePath = getAcutalFilePath();
    TsFileOutput out;
    try {
      if (actualFilePath.getFileSystem().isDistributedFS()) {
        // HDFS
        out =
            new HDFSOutput(
                new org.apache.hadoop.fs.Path(new URI(actualFilePath.getPath())), hadoopConf, true);
      } else {
        // Local File System
        fos = new FileOutputStream(actualFilePath.getPath());
        out = new LocalTsFileOutput(fos);
      }
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    writer = new TsFileWriter(out, schema);
  }

  @Override
  public void close() throws IOException {
    super.close();
    try {
      if (writer != null) {
        writer.close();
        writer = null;
      }
    } finally {
      if (fos != null) {
        fos.close();
        fos = null;
      }
    }
  }

  @Override
  protected String getDirectoryFileName(int taskNumber) {
    return super.getDirectoryFileName(taskNumber) + ".tsfile";
  }

  protected Path getAcutalFilePath() {
    try {
      Field field = FileOutputFormat.class.getDeclaredField("actualFilePath");
      field.setAccessible(true);
      return (Path) field.get(this);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException("Get actual file path failed!", e);
    }
  }

  public Schema getSchema() {
    return schema;
  }

  public Optional<TSFileConfig> getConfig() {
    return Optional.ofNullable(config);
  }
}
