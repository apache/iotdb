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

package org.apache.iotdb.db.schemaengine.schemaregion.read.req.impl;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.commons.schema.template.Template;
import org.apache.iotdb.db.schemaengine.schemaregion.read.req.IShowTimeSeriesPlan;

import java.util.Map;
import java.util.Objects;

public class ShowTimeSeriesPlanImpl extends AbstractShowSchemaPlanImpl
    implements IShowTimeSeriesPlan {

  private final Map<Integer, Template> relatedTemplate;

  private final SchemaFilter schemaFilter;
  private final boolean needViewDetail;

  public ShowTimeSeriesPlanImpl(
      PartialPath path,
      Map<Integer, Template> relatedTemplate,
      long limit,
      long offset,
      boolean isPrefixMatch,
      SchemaFilter schemaFilter,
      boolean needViewDetail,
      PathPatternTree scope) {
    super(path, limit, offset, isPrefixMatch, scope);
    this.relatedTemplate = relatedTemplate;
    this.schemaFilter = schemaFilter;
    this.needViewDetail = needViewDetail;
  }

  @Override
  public boolean needViewDetail() {
    return needViewDetail;
  }

  @Override
  public SchemaFilter getSchemaFilter() {
    return schemaFilter;
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
    return Objects.equals(relatedTemplate, that.relatedTemplate)
        && Objects.equals(schemaFilter, that.schemaFilter);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), relatedTemplate, schemaFilter);
  }
}
