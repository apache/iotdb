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

import org.apache.iotdb.commons.schema.filter.impl.AndFilter;
import org.apache.iotdb.commons.schema.filter.impl.DataTypeFilter;
import org.apache.iotdb.commons.schema.filter.impl.PathContainsFilter;
import org.apache.iotdb.commons.schema.filter.impl.TagFilter;
import org.apache.iotdb.commons.schema.filter.impl.ViewTypeFilter;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class SchemaFilter {

  public static void serialize(SchemaFilter schemaFilter, ByteBuffer byteBuffer) {
    if (schemaFilter == null) {
      ReadWriteIOUtils.write(SchemaFilterType.NULL.getCode(), byteBuffer);
    } else {
      ReadWriteIOUtils.write(schemaFilter.getSchemaFilterType().getCode(), byteBuffer);
      schemaFilter.serialize(byteBuffer);
    }
  }

  public static void serialize(SchemaFilter schemaFilter, DataOutputStream outputStream)
      throws IOException {
    if (schemaFilter == null) {
      ReadWriteIOUtils.write(SchemaFilterType.NULL.getCode(), outputStream);
    } else {
      ReadWriteIOUtils.write(schemaFilter.getSchemaFilterType().getCode(), outputStream);
      schemaFilter.serialize(outputStream);
    }
  }

  public static SchemaFilter deserialize(ByteBuffer byteBuffer) {
    SchemaFilterType type =
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
      default:
        throw new IllegalArgumentException("Unsupported schema filter type: " + type);
    }
  }

  public abstract <C> boolean accept(SchemaFilterVisitor<C> visitor, C node);

  public abstract SchemaFilterType getSchemaFilterType();

  public abstract void serialize(ByteBuffer byteBuffer);

  public abstract void serialize(DataOutputStream stream) throws IOException;
}
