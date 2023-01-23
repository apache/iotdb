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

package org.apache.iotdb.db.mpp.plan.statement.metadata;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.StatementType;
import org.apache.iotdb.db.mpp.plan.statement.StatementVisitor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * CREATE TIMESERIES statement.
 *
 * <p>Here is the syntax definition:
 *
 * <p>CREATE TIMESERIES path [alias] dataType [ENCODING = encodingValue] <br>
 * [COMPRESSOR = compressorValue] [key=value [key=value]...] <br>
 * [TAGS(key=value [, key=value]...)] [ATTRIBUTES(key=value [,key=value]...)]
 */
public class CreateTimeSeriesStatement extends Statement {

  private PartialPath path;
  private String alias;
  private TSDataType dataType;
  private TSEncoding encoding;
  private CompressionType compressor;
  private Map<String, String> props = null;
  private Map<String, String> attributes = null;
  private Map<String, String> tags = null;

  public CreateTimeSeriesStatement() {
    super();
    statementType = StatementType.CREATE_TIMESERIES;
  }

  @Override
  public List<PartialPath> getPaths() {
    return Collections.singletonList(path);
  }

  public PartialPath getPath() {
    return path;
  }

  public void setPath(PartialPath path) {
    this.path = path;
  }

  public TSDataType getDataType() {
    return dataType;
  }

  public void setDataType(TSDataType dataType) {
    this.dataType = dataType;
  }

  public TSEncoding getEncoding() {
    return encoding;
  }

  public void setEncoding(TSEncoding encoding) {
    this.encoding = encoding;
  }

  public void setCompressor(CompressionType compressor) {
    this.compressor = compressor;
  }

  public CompressionType getCompressor() {
    return compressor;
  }

  public Map<String, String> getProps() {
    return props;
  }

  public void setProps(Map<String, String> props) {
    this.props = props;
  }

  public Map<String, String> getAttributes() {
    return attributes;
  }

  public void setAttributes(Map<String, String> attributes) {
    this.attributes = attributes;
  }

  public String getAlias() {
    return alias;
  }

  public void setAlias(String alias) {
    this.alias = alias;
  }

  public Map<String, String> getTags() {
    return tags;
  }

  public void setTags(Map<String, String> tags) {
    this.tags = tags;
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitCreateTimeseries(this, context);
  }
}
