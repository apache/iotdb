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
 *
 */

package org.apache.iotdb.db.metadata.plan.schemaregion.impl.read;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.plan.schemaregion.read.IShowTimeSeriesPlan;
import org.apache.iotdb.db.metadata.template.Template;

import java.util.Map;
import java.util.Objects;

public class ShowTimeSeriesPlanImpl extends AbstractShowSchemaPlanImpl
    implements IShowTimeSeriesPlan {

  private final Map<Integer, Template> relatedTemplate;

  private final boolean isContains;
  private final String key;
  private final String value;

  ShowTimeSeriesPlanImpl(
      PartialPath path,
      Map<Integer, Template> relatedTemplate,
      boolean isContains,
      String key,
      String value,
      long limit,
      long offset,
      boolean isPrefixMatch) {
    super(path, limit, offset, isPrefixMatch);
    this.relatedTemplate = relatedTemplate;
    this.isContains = isContains;
    this.key = key;
    this.value = value;
  }

  @Override
  public boolean isContains() {
    return isContains;
  }

  @Override
  public String getKey() {
    return key;
  }

  @Override
  public String getValue() {
    return value;
  }

  @Override
  public Map<Integer, Template> getRelatedTemplate() {
    return relatedTemplate;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    ShowTimeSeriesPlanImpl that = (ShowTimeSeriesPlanImpl) o;
    return isContains == that.isContains
        && Objects.equals(relatedTemplate, that.relatedTemplate)
        && Objects.equals(key, that.key)
        && Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), relatedTemplate, isContains, key, value);
  }
}
