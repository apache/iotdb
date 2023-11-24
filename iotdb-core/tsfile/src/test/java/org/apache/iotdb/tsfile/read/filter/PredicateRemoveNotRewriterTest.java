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

import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.read.filter.factory.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.factory.ValueFilter;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;

public class PredicateRemoveNotRewriterTest {

  @Test
  public void testReverse() {
    Assert.assertEquals(TimeFilter.gt(1), TimeFilter.ltEq(1).reverse());
    Assert.assertEquals(TimeFilter.gtEq(1), TimeFilter.lt(1).reverse());
    Assert.assertEquals(TimeFilter.lt(1), TimeFilter.gtEq(1).reverse());
    Assert.assertEquals(TimeFilter.ltEq(1), TimeFilter.gt(1).reverse());
    Assert.assertEquals(TimeFilter.eq(1), TimeFilter.notEq(1).reverse());
    Assert.assertEquals(TimeFilter.notEq(1), TimeFilter.eq(1).reverse());
    Assert.assertEquals(ValueFilter.like("s*"), ValueFilter.notLike("s*").reverse());
    Assert.assertEquals(ValueFilter.notLike("s*"), ValueFilter.like("s*").reverse());
    Assert.assertEquals(ValueFilter.regexp("s*"), ValueFilter.notRegexp("s*").reverse());
    Assert.assertEquals(ValueFilter.notRegexp("s*"), ValueFilter.regexp("s*").reverse());
    Assert.assertEquals(TimeFilter.between(1, 100), TimeFilter.notBetween(1, 100).reverse());
    Assert.assertEquals(TimeFilter.notBetween(1, 100), TimeFilter.between(1, 100).reverse());
    Assert.assertEquals(
        ValueFilter.in(new HashSet<>(Arrays.asList("a", "b"))),
        ValueFilter.notIn(new HashSet<>(Arrays.asList("a", "b"))).reverse());
    Assert.assertEquals(
        ValueFilter.notIn(new HashSet<>(Arrays.asList("a", "b"))),
        ValueFilter.in(new HashSet<>(Arrays.asList("a", "b"))).reverse());
    Assert.assertEquals(TimeFilter.gt(1), FilterFactory.not(TimeFilter.gt(1)).reverse());
    Assert.assertEquals(
        FilterFactory.and(TimeFilter.gt(1), TimeFilter.ltEq(1)),
        FilterFactory.or(TimeFilter.ltEq(1), TimeFilter.gt(1)).reverse());
    Assert.assertEquals(
        FilterFactory.or(TimeFilter.ltEq(1), TimeFilter.gt(1)),
        FilterFactory.and(TimeFilter.gt(1), TimeFilter.ltEq(1)).reverse());
  }

  @Test
  public void testRemoveNot() {
    Assert.assertEquals(
        TimeFilter.ltEq(1),
        PredicateRemoveNotRewriter.rewrite(FilterFactory.not(TimeFilter.gt(1))));
    Assert.assertEquals(
        ValueFilter.like("s*"),
        PredicateRemoveNotRewriter.rewrite(FilterFactory.not(ValueFilter.notLike("s*"))));
    Assert.assertEquals(
        FilterFactory.or(TimeFilter.gt(1), TimeFilter.ltEq(1)),
        PredicateRemoveNotRewriter.rewrite(
            FilterFactory.or(
                FilterFactory.not(TimeFilter.ltEq(1)), FilterFactory.not(TimeFilter.gt(1)))));
    Assert.assertEquals(
        FilterFactory.and(TimeFilter.gt(1), TimeFilter.ltEq(1)),
        PredicateRemoveNotRewriter.rewrite(
            FilterFactory.and(
                FilterFactory.not(TimeFilter.ltEq(1)), FilterFactory.not(TimeFilter.gt(1)))));
  }
}
