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
import org.apache.iotdb.hadoop.fileSystem.HDFSInput;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.read.ReadOnlyTsFile;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.read.reader.LocalTsFileInput;
import org.apache.iotdb.tsfile.read.reader.TsFileInput;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.util.HadoopUtils;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Optional;

/**
 * Input format that reads TsFiles. Users need to provide a {@link RowRecordParser} used to parse
 * the raw data read from TsFiles into the type T.
 *
 * @param <T> The output type of this input format.
 */
public class TsFileInputFormat<T> extends FileInputFormat<T> implements ResultTypeQueryable<T> {

  private final QueryExpression expression;
  private final RowRecordParser<T> parser;
  @Nullable private final TSFileConfig config;

  private transient org.apache.hadoop.conf.Configuration hadoopConf = null;
  private transient ReadOnlyTsFile readTsFile = null;
  private transient QueryDataSet queryDataSet = null;

  public TsFileInputFormat(
      @Nullable String path,
      QueryExpression expression,
      RowRecordParser<T> parser,
      @Nullable TSFileConfig config) {
    super(path != null ? new Path(path) : null);
    this.expression = expression;
    this.parser = parser;
    this.config = config;
  }

  public TsFileInputFormat(
      @Nullable String path, QueryExpression expression, RowRecordParser<T> parser) {
    this(path, expression, parser, null);
  }

  public TsFileInputFormat(QueryExpression expression, RowRecordParser<T> parser) {
    this(null, expression, parser, null);
  }

  @Override
  public void configure(Configuration flinkConfiguration) {
    super.configure(flinkConfiguration);
    hadoopConf = HadoopUtils.getHadoopConfiguration(flinkConfiguration);
  }

  @Override
  public void open(FileInputSplit split) throws IOException {
    super.open(split);
    if (config != null) {
      TSFileConfigUtil.setGlobalTSFileConfig(config);
    }
    TsFileInput in;
    try {
      if (currentSplit.getPath().getFileSystem().isDistributedFS()) {
        // HDFS
        in =
            new HDFSInput(
                new org.apache.hadoop.fs.Path(new URI(currentSplit.getPath().getPath())),
                hadoopConf);
      } else {
        // Local File System
        in = new LocalTsFileInput(Paths.get(currentSplit.getPath().toUri()));
      }
    } catch (URISyntaxException e) {
      throw new FlinkRuntimeException(e);
    }
    try (TsFileSequenceReader reader = new TsFileSequenceReader(in)) {
      readTsFile = new ReadOnlyTsFile(reader);
      queryDataSet =
          readTsFile.query(
              // The query method call will change the content of the param query expression,
              // the original query expression should not be passed to the query method as it may
              // be used several times.
              QueryExpression.create(expression.getSelectedSeries(), expression.getExpression()),
              currentSplit.getStart(),
              currentSplit.getStart() + currentSplit.getLength());
    }
  }

  @Override
  public void close() throws IOException {
    super.close();
    if (readTsFile != null) {
      readTsFile.close();
      readTsFile = null;
    }
  }

  @Override
  public boolean reachedEnd() throws IOException {
    return !queryDataSet.hasNext();
  }

  @Override
  public T nextRecord(T t) throws IOException {
    RowRecord rowRecord = queryDataSet.next();
    return parser.parse(rowRecord, t);
  }

  @Override
  public boolean supportsMultiPaths() {
    return true;
  }

  public QueryExpression getExpression() {
    return expression;
  }

  public RowRecordParser<T> getParser() {
    return parser;
  }

  public Optional<TSFileConfig> getConfig() {
    return Optional.ofNullable(config);
  }

  @Override
  public TypeInformation<T> getProducedType() {
    if (this.getParser() instanceof ResultTypeQueryable) {
      return ((ResultTypeQueryable) this.getParser()).getProducedType();
    } else {
      return TypeExtractor.createTypeInfo(
          RowRecordParser.class, this.getParser().getClass(), 0, null, null);
    }
  }
}
