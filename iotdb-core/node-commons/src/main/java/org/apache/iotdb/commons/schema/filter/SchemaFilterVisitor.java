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
 * "AS IS" BASIS, WITHOUT WARRANTIES OBoolean CONDITIONS OF ANY
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
import org.apache.iotdb.commons.schema.filter.impl.values.ComparisonFilter;
import org.apache.iotdb.commons.schema.filter.impl.values.InFilter;
import org.apache.iotdb.commons.schema.filter.impl.values.LikeFilter;
import org.apache.iotdb.commons.schema.filter.impl.values.PreciseFilter;

import java.util.Objects;

/**
 * This class provides a visitor of {@link SchemaFilter}, which can be extended to create a visitor
 * which only needs to handle a subset of the available methods.
 */
public abstract class SchemaFilterVisitor<C> {

  public Boolean process(SchemaFilter filter, C context) {
    if (filter == null) {
      return visitNode(null, context);
    } else {
      return filter.accept(this, context);
    }
  }

  /** Top Level Description */
  protected abstract Boolean visitNode(SchemaFilter filter, C context);

  public Boolean visitFilter(SchemaFilter filter, C context) {
    return visitNode(filter, context);
  }

  public Boolean visitTagFilter(TagFilter tagFilter, C context) {
    return visitFilter(tagFilter, context);
  }

  public Boolean visitPathContainsFilter(PathContainsFilter pathContainsFilter, C context) {
    return visitFilter(pathContainsFilter, context);
  }

  public Boolean visitDataTypeFilter(DataTypeFilter dataTypeFilter, C context) {
    return visitFilter(dataTypeFilter, context);
  }

  public Boolean visitViewTypeFilter(ViewTypeFilter viewTypeFilter, C context) {
    return visitFilter(viewTypeFilter, context);
  }

  public Boolean visitTemplateFilter(TemplateFilter templateFilter, C context) {
    return visitFilter(templateFilter, context);
  }

  public Boolean visitAndFilter(final AndFilter andFilter, final C context) {
    Boolean result = Boolean.TRUE;
    for (final SchemaFilter child : andFilter.getChildren()) {
      final Boolean childResult = child.accept(this, context);
      if (Boolean.FALSE.equals(childResult)) {
        return Boolean.FALSE;
      }
      if (Objects.isNull(childResult)) {
        result = null;
      }
    }
    return result;
  }

  public final Boolean visitOrFilter(final OrFilter orFilter, final C context) {
    Boolean result = Boolean.FALSE;
    for (final SchemaFilter child : orFilter.getChildren()) {
      final Boolean childResult = child.accept(this, context);
      if (Boolean.TRUE.equals(childResult)) {
        return Boolean.TRUE;
      }
      if (Objects.isNull(childResult)) {
        result = null;
      }
    }
    return result;
  }

  public final Boolean visitNotFilter(final NotFilter notFilter, final C context) {
    final Boolean result = notFilter.getChild().accept(this, context);
    if (Boolean.TRUE.equals(result)) {
      return Boolean.FALSE;
    }
    if (Boolean.FALSE.equals(result)) {
      return Boolean.TRUE;
    }
    return result;
  }

  public Boolean visitIdFilter(final IdFilter filter, final C context) {
    return visitFilter(filter, context);
  }

  public Boolean visitAttributeFilter(final AttributeFilter filter, final C context) {
    return visitFilter(filter, context);
  }

  public Boolean visitPreciseFilter(final PreciseFilter filter, final C context) {
    return visitFilter(filter, context);
  }

  public Boolean visitComparisonFilter(final ComparisonFilter filter, final C context) {
    return visitFilter(filter, context);
  }

  public Boolean visitInFilter(final InFilter filter, final C context) {
    return visitFilter(filter, context);
  }

  public Boolean visitLikeFilter(final LikeFilter filter, final C context) {
    return visitFilter(filter, context);
  }
}
