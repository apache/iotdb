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
package org.apache.iotdb.tsfile.read.filter;

import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class FilterSerializeTest {

  @Test
  public void testValueFilter() {
    Filter[] filters =
        new Filter[] {
          ValueFilter.eq(1),
          ValueFilter.gt(2L),
          ValueFilter.gtEq("filter"),
          ValueFilter.lt(0.1),
          ValueFilter.ltEq(0.01f),
          ValueFilter.not(ValueFilter.eq(true)),
          ValueFilter.notEq(false)
        };
    for (Filter filter : filters) {
      validateSerialization(filter);
    }
  }

  @Test
  public void testTimeFilter() {
    Filter[] filters =
        new Filter[] {
          TimeFilter.eq(1),
          TimeFilter.gt(2),
          TimeFilter.gtEq(3),
          TimeFilter.lt(4),
          TimeFilter.ltEq(5),
          TimeFilter.not(ValueFilter.eq(6)),
          TimeFilter.notEq(7)
        };
    for (Filter filter : filters) {
      validateSerialization(filter);
    }
  }

  @Test
  public void testBinaryFilter() {
    Filter[] filters =
        new Filter[] {
          FilterFactory.and(TimeFilter.eq(1), ValueFilter.eq(1)),
          FilterFactory.or(ValueFilter.gt(2L), TimeFilter.not(ValueFilter.eq(6)))
        };
    for (Filter filter : filters) {
      validateSerialization(filter);
    }
  }

  @Test
  public void testGroupByFilter() {
    Filter[] filters =
        new Filter[] {
          new GroupByFilter(1, 2, 3, 4), new GroupByFilter(4, 3, 2, 1),
        };
    for (Filter filter : filters) {
      validateSerialization(filter);
    }
  }

  private void validateSerialization(Filter filter) {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
    filter.serialize(dataOutputStream);

    ByteBuffer buffer = ByteBuffer.wrap(outputStream.toByteArray());
    Filter serialized = FilterFactory.deserialize(buffer);
    assertEquals(filter, serialized);
  }
}
