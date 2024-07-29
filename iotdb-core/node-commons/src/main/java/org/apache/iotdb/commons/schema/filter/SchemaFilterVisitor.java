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
 * "AS IS" BASIS, WITHOUT WARRANTIES Oboolean CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.commons.schema.filter;

import org.apache.iotdb.commons.schema.filter.impl.DataTypeFilter;
import org.apache.iotdb.commons.schema.filter.impl.PathContainsFilter;
import org.apache.iotdb.commons.schema.filter.impl.TagFilter;
import org.apache.iotdb.commons.schema.filter.impl.TemplateFilter;
import org.apache.iotdb.commons.schema.filter.impl.ViewTypeFilter;
import org.apache.iotdb.commons.schema.filter.impl.multichildren.AndFilter;
import org.apache.iotdb.commons.schema.filter.impl.multichildren.OrFilter;
import org.apache.iotdb.commons.schema.filter.impl.singlechild.AttributeFilter;
import org.apache.iotdb.commons.schema.filter.impl.singlechild.IdFilter;
import org.apache.iotdb.commons.schema.filter.impl.singlechild.NotFilter;
import org.apache.iotdb.commons.schema.filter.impl.values.InFilter;
import org.apache.iotdb.commons.schema.filter.impl.values.LikeFilter;
import org.apache.iotdb.commons.schema.filter.impl.values.PreciseFilter;

/**
 * This class provides a visitor of {@link SchemaFilter}, which can be extended to create a visitor
 * which only needs to handle a subset of the available methods.
 */
public abstract class SchemaFilterVisitor<C> {

  public boolean process(SchemaFilter filter, C context) {
    if (filter == null) {
      return visitNode(null, context);
    } else {
      return filter.accept(this, context);
    }
  }

  /** Top Level Description */
  protected abstract boolean visitNode(SchemaFilter filter, C context);

  public boolean visitFilter(SchemaFilter filter, C context) {
    return visitNode(filter, context);
  }

  public boolean visitTagFilter(TagFilter tagFilter, C context) {
    return visitFilter(tagFilter, context);
  }

  public boolean visitPathContainsFilter(PathContainsFilter pathContainsFilter, C context) {
    return visitFilter(pathContainsFilter, context);
  }

  public boolean visitDataTypeFilter(DataTypeFilter dataTypeFilter, C context) {
    return visitFilter(dataTypeFilter, context);
  }

  public boolean visitViewTypeFilter(ViewTypeFilter viewTypeFilter, C context) {
    return visitFilter(viewTypeFilter, context);
  }

  public boolean visitTemplateFilter(TemplateFilter templateFilter, C context) {
    return visitFilter(templateFilter, context);
  }

  public final boolean visitAndFilter(final AndFilter andFilter, final C context) {
    return andFilter.getChildren().stream().allMatch(child -> child.accept(this, context));
  }

  public final boolean visitOrFilter(final OrFilter orFilter, final C context) {
    return orFilter.getChildren().stream().anyMatch(child -> child.accept(this, context));
  }

  public final boolean visitNotFilter(final NotFilter notFilter, final C context) {
    return !notFilter.getChild().accept(this, context);
  }

  public boolean visitIdFilter(final IdFilter filter, final C context) {
    return visitFilter(filter, context);
  }

  public boolean visitAttributeFilter(final AttributeFilter filter, final C context) {
    return visitFilter(filter, context);
  }

  public boolean visitPreciseFilter(final PreciseFilter filter, final C context) {
    return visitFilter(filter, context);
  }

  public boolean visitInFilter(final InFilter filter, final C context) {
    return visitFilter(filter, context);
  }

  public boolean visitLikeFilter(final LikeFilter filter, final C context) {
    return visitFilter(filter, context);
  }
}
