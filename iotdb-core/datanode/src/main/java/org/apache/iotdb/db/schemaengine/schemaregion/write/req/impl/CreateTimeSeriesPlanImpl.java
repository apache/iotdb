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
package org.apache.iotdb.db.schemaengine.schemaregion.write.req.impl;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.ICreateTimeSeriesPlan;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.util.Map;
import java.util.TreeMap;

public class CreateTimeSeriesPlanImpl implements ICreateTimeSeriesPlan {

  private PartialPath path;
  private TSDataType dataType;
  private TSEncoding encoding;
  private CompressionType compressor;
  private String alias;
  private Map<String, String> props = null;
  private Map<String, String> tags = null;
  private Map<String, String> attributes = null;
  private long tagOffset = -1;
  private transient boolean withMerge;

  public CreateTimeSeriesPlanImpl() {}

  public CreateTimeSeriesPlanImpl(
      PartialPath path,
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compressor,
      Map<String, String> props,
      Map<String, String> tags,
      Map<String, String> attributes,
      String alias) {
    this.path = path;
    this.dataType = dataType;
    this.encoding = encoding;
    this.compressor = compressor;
    this.tags = tags;
    this.attributes = attributes;
    this.alias = alias;
    if (props != null) {
      this.props = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
      this.props.putAll(props);
    }
  }

  public CreateTimeSeriesPlanImpl(PartialPath path, MeasurementSchema schema) {
    this.path = path;
    this.dataType = schema.getType();
    this.encoding = schema.getEncodingType();
    this.compressor = schema.getCompressor();
  }

  @Override
  public PartialPath getPath() {
    return path;
  }

  @Override
  public void setPath(PartialPath path) {
    this.path = path;
  }

  @Override
  public TSDataType getDataType() {
    return dataType;
  }

  @Override
  public void setDataType(TSDataType dataType) {
    this.dataType = dataType;
  }

  @Override
  public CompressionType getCompressor() {
    return compressor;
  }

  @Override
  public void setCompressor(CompressionType compressor) {
    this.compressor = compressor;
  }

  @Override
  public TSEncoding getEncoding() {
    return encoding;
  }

  @Override
  public void setEncoding(TSEncoding encoding) {
    this.encoding = encoding;
  }

  @Override
  public Map<String, String> getAttributes() {
    return attributes;
  }

  @Override
  public void setAttributes(Map<String, String> attributes) {
    this.attributes = attributes;
  }

  @Override
  public String getAlias() {
    return alias;
  }

  @Override
  public void setAlias(String alias) {
    this.alias = alias;
  }

  @Override
  public Map<String, String> getTags() {
    return tags;
  }

  @Override
  public void setTags(Map<String, String> tags) {
    this.tags = tags;
  }

  @Override
  public Map<String, String> getProps() {
    return props;
  }

  @Override
  public void setProps(Map<String, String> props) {
    this.props = props;
  }

  @Override
  public long getTagOffset() {
    return tagOffset;
  }

  @Override
  public void setTagOffset(long tagOffset) {
    this.tagOffset = tagOffset;
  }

  public boolean isWithMerge() {
    return withMerge;
  }

  public void setWithMerge(final boolean withMerge) {
    this.withMerge = withMerge;
  }
}
