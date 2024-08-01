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

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public abstract class SchemaFilter {

  public static void serialize(final SchemaFilter schemaFilter, final ByteBuffer byteBuffer) {
    if (schemaFilter == null) {
      ReadWriteIOUtils.write(SchemaFilterType.NULL.getCode(), byteBuffer);
    } else {
      ReadWriteIOUtils.write(schemaFilter.getSchemaFilterType().getCode(), byteBuffer);
      schemaFilter.serialize(byteBuffer);
    }
  }

  public static void serialize(final SchemaFilter schemaFilter, final DataOutputStream outputStream)
      throws IOException {
    if (schemaFilter == null) {
      ReadWriteIOUtils.write(SchemaFilterType.NULL.getCode(), outputStream);
    } else {
      ReadWriteIOUtils.write(schemaFilter.getSchemaFilterType().getCode(), outputStream);
      schemaFilter.serialize(outputStream);
    }
  }

  public static SchemaFilter deserialize(final ByteBuffer byteBuffer) {
    final SchemaFilterType type =
        SchemaFilterType.getSchemaFilterType(ReadWriteIOUtils.readShort(byteBuffer));
    switch (type) {
      case NULL:
        return null;
      case TAGS_FILTER:
        return new TagFilter(byteBuffer);
      case PATH_CONTAINS:
        return new PathContainsFilter(byteBuffer);
      case DATA_TYPE:
        return new DataTypeFilter(byteBuffer);
      case VIEW_TYPE:
        return new ViewTypeFilter(byteBuffer);
      case AND:
        return new AndFilter(byteBuffer);
      case TEMPLATE_FILTER:
        return new TemplateFilter(byteBuffer);
      case OR:
        return new OrFilter(byteBuffer);
      case NOT:
        return new NotFilter(byteBuffer);
      case ID:
        return new IdFilter(byteBuffer);
      case ATTRIBUTE:
        return new AttributeFilter(byteBuffer);
      case PRECISE:
        return new PreciseFilter(byteBuffer);
      case IN:
        return new InFilter(byteBuffer);
      case LIKE:
        return new LikeFilter(byteBuffer);
      case COMPARISON:
        return new ComparisonFilter(byteBuffer);
      default:
        throw new IllegalArgumentException("Unsupported schema filter type: " + type);
    }
  }

  /**
   * Extracts a certain type of SchemaFilter from the Filter tree.
   *
   * @param filterType type
   * @return list of SchemaFilter with specific type
   */
  public static List<SchemaFilter> extract(
      final SchemaFilter schemaFilter, final SchemaFilterType filterType) {
    final List<SchemaFilter> res = new ArrayList<>();
    internalExtract(res, schemaFilter, filterType);
    return res;
  }

  private static void internalExtract(
      final List<SchemaFilter> result,
      final SchemaFilter schemaFilter,
      final SchemaFilterType filterType) {
    if (schemaFilter.getSchemaFilterType().equals(filterType)) {
      result.add(schemaFilter);
    }
    if (schemaFilter.getSchemaFilterType().equals(SchemaFilterType.AND)) {
      final AndFilter andFilter = (AndFilter) schemaFilter;
      andFilter.getChildren().forEach(child -> internalExtract(result, child, filterType));
    }
  }

  public abstract <C> Boolean accept(final SchemaFilterVisitor<C> visitor, C node);

  public abstract SchemaFilterType getSchemaFilterType();

  protected abstract void serialize(final ByteBuffer byteBuffer);

  protected abstract void serialize(final DataOutputStream stream) throws IOException;
}
