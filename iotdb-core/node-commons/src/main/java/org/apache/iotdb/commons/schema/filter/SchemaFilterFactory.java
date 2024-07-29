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
import org.apache.iotdb.commons.schema.view.ViewType;

import org.apache.tsfile.enums.TSDataType;

import java.util.Arrays;

public class SchemaFilterFactory {

  public static SchemaFilter createTagFilter(String key, String value, boolean isContains) {
    return new TagFilter(key, value, isContains);
  }

  public static SchemaFilter createPathContainsFilter(String containString) {
    return new PathContainsFilter(containString);
  }

  public static SchemaFilter createDataTypeFilter(TSDataType dataType) {
    return new DataTypeFilter(dataType);
  }

  public static SchemaFilter createViewTypeFilter(ViewType viewType) {
    return new ViewTypeFilter(viewType);
  }

  public static SchemaFilter createTemplateNameFilter(String templateName, boolean isEqual) {
    return new TemplateFilter(templateName, isEqual);
  }

  public static SchemaFilter and(final SchemaFilter left, final SchemaFilter right) {
    if (left == null) {
      return right;
    } else if (right == null) {
      return left;
    } else {
      return new AndFilter(Arrays.asList(left, right));
    }
  }
}
