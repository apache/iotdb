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

package org.apache.iotdb.commons.schema;

import org.apache.iotdb.commons.schema.filter.SchemaFilter;
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
import org.apache.iotdb.commons.schema.view.ViewType;

import org.apache.tsfile.enums.TSDataType;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;

import static org.apache.tsfile.utils.RegexUtils.parseLikePatternToRegex;

public class SchemaFilterSerDeTest {

  @Test
  public void testSchemaFilter() {
    final TagFilter tagFilter = new TagFilter("1", "2", true);
    final PathContainsFilter pathContainsFilter = new PathContainsFilter("fakePath");
    final DataTypeFilter dataTypeFilter = new DataTypeFilter(TSDataType.BOOLEAN);
    final ViewTypeFilter viewTypeFilter = new ViewTypeFilter(ViewType.VIEW);
    final TemplateFilter templateFilter = new TemplateFilter("t1", false);
    final NotFilter notFilter = new NotFilter(tagFilter);
    final AndFilter andFilter =
        new AndFilter(Arrays.asList(dataTypeFilter, viewTypeFilter, templateFilter));
    final OrFilter orFilter =
        new OrFilter(Arrays.asList(viewTypeFilter, pathContainsFilter, tagFilter));
    final PreciseFilter preciseFilter = new PreciseFilter("s1");
    final InFilter inFilter = new InFilter(Collections.singleton("d1"));
    final LikeFilter likeFilter = new LikeFilter(parseLikePatternToRegex("__1"));
    final IdFilter idFilter = new IdFilter(preciseFilter, 1);
    final AttributeFilter attributeFilter = new AttributeFilter(likeFilter, "attr");
    final ComparisonFilter comparisonFilter =
        new ComparisonFilter(ComparisonFilter.Operator.GREATER_THAN, "s1");

    serDeTest(tagFilter);
    serDeTest(pathContainsFilter);
    serDeTest(dataTypeFilter);
    serDeTest(viewTypeFilter);
    serDeTest(templateFilter);
    serDeTest(notFilter);
    serDeTest(andFilter);
    serDeTest(orFilter);
    serDeTest(preciseFilter);
    serDeTest(inFilter);
    serDeTest(likeFilter);
    serDeTest(idFilter);
    serDeTest(attributeFilter);
    serDeTest(comparisonFilter);
  }

  private void serDeTest(final SchemaFilter filter) {
    final ByteBuffer buffer = ByteBuffer.allocate(1000);
    SchemaFilter.serialize(filter, buffer);
    buffer.flip();
    Assert.assertEquals(filter, SchemaFilter.deserialize(buffer));
  }
}
