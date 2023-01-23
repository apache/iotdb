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

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * ALTER TIMESERIES statement.
 *
 * <p>Here is the syntax definition:
 *
 * <p>ALTER TIMESERIES path RENAME | SET | DROP | ADD TAGS | ADD ATTRIBUTES | UPSERT
 */
public class AlterTimeSeriesStatement extends Statement {
  private PartialPath path;
  private AlterTimeSeriesStatement.AlterType alterType;

  /**
   * used when the alterType is RENAME, SET, DROP, ADD_TAGS, ADD_ATTRIBUTES when the alterType is
   * RENAME, alterMap has only one entry, key is the previousName, value is the currentName when the
   * alterType is DROP, only the keySet of alterMap is useful, it contains all the key names needed
   * to be removed
   */
  private Map<String, String> alterMap;

  /** used when the alterType is UPSERT */
  private String alias;

  private Map<String, String> tagsMap;
  private Map<String, String> attributesMap;

  public AlterTimeSeriesStatement() {
    super();
    statementType = StatementType.ALTER_TIMESERIES;
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

  public String getAlias() {
    return alias;
  }

  public void setAlias(String alias) {
    this.alias = alias;
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

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitAlterTimeseries(this, context);
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
