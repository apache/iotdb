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

package org.apache.iotdb.db.qp.physical.sys;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.sys.AlterTimeSeriesOperator;
import org.apache.iotdb.db.qp.logical.sys.AlterTimeSeriesOperator.AlterType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;

public class AlterTimeSeriesPlan extends PhysicalPlan {

  private final PartialPath path;

  private final AlterTimeSeriesOperator.AlterType alterType;

  // used when the alterType is RENAME, SET, DROP, ADD_TAGS, ADD_ATTRIBUTES
  // when the alterType is RENAME, alterMap has only one entry, key is the beforeName, value is the
  // currentName
  // when the alterType is DROP, only the keySet of alterMap is useful, it contains all the key
  // names needed to be removed
  private final Map<String, String> alterMap;

  // used when the alterType is UPSERT
  private final String alias;
  private final Map<String, String> tagsMap;
  private final Map<String, String> attributesMap;

  public AlterTimeSeriesPlan(PartialPath path, AlterType alterType, Map<String, String> alterMap,
      String alias, Map<String, String> tagsMap, Map<String, String> attributesMap) {
    super(false, Operator.OperatorType.ALTER_TIMESERIES);
    this.path = path;
    this.alterType = alterType;
    this.alterMap = alterMap;
    this.alias = alias;
    this.tagsMap = tagsMap;
    this.attributesMap = attributesMap;
  }

  public PartialPath getPath() {
    return path;
  }

  public AlterTimeSeriesOperator.AlterType getAlterType() {
    return alterType;
  }

  public Map<String, String> getAlterMap() {
    return alterMap;
  }

  public String getAlias() {
    return alias;
  }

  public Map<String, String> getTagsMap() {
    return tagsMap;
  }

  public Map<String, String> getAttributesMap() {
    return attributesMap;
  }

  @Override
  public List<PartialPath> getPaths() {
    return Collections.singletonList(path);
  }
}
