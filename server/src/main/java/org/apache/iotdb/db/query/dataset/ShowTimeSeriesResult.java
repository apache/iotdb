/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.query.dataset;

import java.util.Map;

public class ShowTimeSeriesResult {

  private String name;
  private String alias;
  private String sgName;
  private String dataType;
  private String encoding;
  private String compressor;
  private Map<String, String> tagAndAttribute;

  public ShowTimeSeriesResult(String name, String alias, String sgName, String dataType,
      String encoding, String compressor, Map<String, String> tagAndAttribute) {
    this.name = name;
    this.alias = alias;
    this.sgName = sgName;
    this.dataType = dataType;
    this.encoding = encoding;
    this.compressor = compressor;
    this.tagAndAttribute = tagAndAttribute;
  }

  public String getName() {
    return name;
  }

  public String getAlias() {
    return alias;
  }

  public String getSgName() {
    return sgName;
  }

  public String getDataType() {
    return dataType;
  }

  public String getEncoding() {
    return encoding;
  }

  public String getCompressor() {
    return compressor;
  }

  public Map<String, String> getTagAndAttribute() {
    return tagAndAttribute;
  }
}