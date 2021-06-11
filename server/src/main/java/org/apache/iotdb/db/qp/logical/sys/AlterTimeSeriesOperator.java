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

package org.apache.iotdb.db.qp.logical.sys;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.AlterTimeSeriesPlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;

import java.util.Map;

public class AlterTimeSeriesOperator extends Operator {

  private PartialPath path;

  private AlterType alterType;

  // used when the alterType is RENAME, SET, DROP, ADD_TAGS, ADD_ATTRIBUTES
  // when the alterType is RENAME, alterMap has only one entry, key is the beforeName, value is the
  // currentName
  // when the alterType is DROP, only the keySet of alterMap is useful, it contains all the key
  // names needed to be removed
  private Map<String, String> alterMap;

  // used when the alterType is UPSERT
  private String alias;
  private Map<String, String> tagsMap;
  private Map<String, String> attributesMap;

  public AlterTimeSeriesOperator(int tokenIntType) {
    super(tokenIntType);
    operatorType = OperatorType.ALTER_TIMESERIES;
  }

  public PartialPath getPath() {
    return path;
  }

  public void setPath(PartialPath path) {
    this.path = path;
  }

  public AlterType getAlterType() {
    return alterType;
  }

  public void setAlterType(AlterType alterType) {
    this.alterType = alterType;
  }

  public Map<String, String> getAlterMap() {
    return alterMap;
  }

  public void setAlterMap(Map<String, String> alterMap) {
    this.alterMap = alterMap;
  }

  public Map<String, String> getTagsMap() {
    return tagsMap;
  }

  public void setTagsMap(Map<String, String> tagsMap) {
    this.tagsMap = tagsMap;
  }

  public Map<String, String> getAttributesMap() {
    return attributesMap;
  }

  public void setAttributesMap(Map<String, String> attributesMap) {
    this.attributesMap = attributesMap;
  }

  public String getAlias() {
    return alias;
  }

  public void setAlias(String alias) {
    this.alias = alias;
  }

  @Override
  public PhysicalPlan generatePhysicalPlan(PhysicalGenerator generator)
      throws QueryProcessException {
    return new AlterTimeSeriesPlan(path, alterType, alterMap, alias, tagsMap, attributesMap);
  }

  public enum AlterType {
    RENAME,
    SET,
    DROP,
    ADD_TAGS,
    ADD_ATTRIBUTES,
    UPSERT
  }
}
