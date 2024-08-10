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

package org.apache.iotdb.commons.schema.filter.impl;

import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.commons.schema.filter.SchemaFilterVisitor;
import org.apache.iotdb.commons.schema.filter.impl.singlechild.AttributeFilter;
import org.apache.iotdb.commons.schema.filter.impl.singlechild.IdFilter;
import org.apache.iotdb.commons.schema.filter.impl.values.ComparisonFilter;
import org.apache.iotdb.commons.schema.filter.impl.values.InFilter;
import org.apache.iotdb.commons.schema.filter.impl.values.LikeFilter;
import org.apache.iotdb.commons.schema.filter.impl.values.PreciseFilter;

import java.util.Objects;

public class StringValueFilterVisitor extends SchemaFilterVisitor<String> {

  @Override
  protected Boolean visitNode(final SchemaFilter filter, final String context) {
    return true;
  }

  @Override
  public Boolean visitPreciseFilter(final PreciseFilter filter, final String context) {
    return Objects.equals(filter.getValue(), context);
  }

  @Override
  public Boolean visitComparisonFilter(final ComparisonFilter filter, final String context) {
    if (Objects.isNull(context)) {
      return null;
    }

    final int result = context.compareTo(filter.getValue());

    switch (filter.getOperator()) {
      case NOT_EQUAL:
        return result != 0;
      case LESS_THAN:
        return result < 0;
      case LESS_THAN_OR_EQUAL:
        return result <= 0;
      case GREATER_THAN:
        return result > 0;
      case GREATER_THAN_OR_EQUAL:
        return result >= 0;
    }
    return false;
  }

  @Override
  public Boolean visitInFilter(final InFilter filter, final String context) {
    if (Objects.isNull(context)) {
      return null;
    }
    return filter.getValues().contains(context);
  }

  @Override
  public Boolean visitLikeFilter(final LikeFilter filter, final String context) {
    if (Objects.isNull(context)) {
      return null;
    }
    return filter.getPattern().matcher(context).find();
  }

  @Override
  public Boolean visitIdFilter(final IdFilter filter, final String context) {
    return filter.getChild().accept(this, context);
  }

  @Override
  public Boolean visitAttributeFilter(final AttributeFilter filter, final String context) {
    return filter.getChild().accept(this, context);
  }

  private static class StringValueFilterVisitorContainer {
    private static final StringValueFilterVisitor instance = new StringValueFilterVisitor();
  }

  public static StringValueFilterVisitor getInstance() {
    return StringValueFilterVisitorContainer.instance;
  }

  private StringValueFilterVisitor() {
    // Instance
  }
}
