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
import org.apache.iotdb.tsfile.read.filter.factory.TimeFilterApi;
import org.apache.iotdb.tsfile.read.filter.factory.ValueFilterApi;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;

public class PredicateRemoveNotRewriterTest {

  @Test
  public void testReverse() {
    Assert.assertEquals(TimeFilterApi.gt(1), TimeFilterApi.ltEq(1).reverse());
    Assert.assertEquals(TimeFilterApi.gtEq(1), TimeFilterApi.lt(1).reverse());
    Assert.assertEquals(TimeFilterApi.lt(1), TimeFilterApi.gtEq(1).reverse());
    Assert.assertEquals(TimeFilterApi.ltEq(1), TimeFilterApi.gt(1).reverse());
    Assert.assertEquals(TimeFilterApi.eq(1), TimeFilterApi.notEq(1).reverse());
    Assert.assertEquals(TimeFilterApi.notEq(1), TimeFilterApi.eq(1).reverse());
    Assert.assertEquals(ValueFilterApi.like("s*"), ValueFilterApi.notLike("s*").reverse());
    Assert.assertEquals(ValueFilterApi.notLike("s*"), ValueFilterApi.like("s*").reverse());
    Assert.assertEquals(ValueFilterApi.regexp("s*"), ValueFilterApi.notRegexp("s*").reverse());
    Assert.assertEquals(ValueFilterApi.notRegexp("s*"), ValueFilterApi.regexp("s*").reverse());
    Assert.assertEquals(TimeFilterApi.between(1, 100), TimeFilterApi.notBetween(1, 100).reverse());
    Assert.assertEquals(TimeFilterApi.notBetween(1, 100), TimeFilterApi.between(1, 100).reverse());
    Assert.assertEquals(
        ValueFilterApi.in(new HashSet<>(Arrays.asList("a", "b"))),
        ValueFilterApi.notIn(new HashSet<>(Arrays.asList("a", "b"))).reverse());
    Assert.assertEquals(
        ValueFilterApi.notIn(new HashSet<>(Arrays.asList("a", "b"))),
        ValueFilterApi.in(new HashSet<>(Arrays.asList("a", "b"))).reverse());
    Assert.assertEquals(TimeFilterApi.gt(1), FilterFactory.not(TimeFilterApi.gt(1)).reverse());
    Assert.assertEquals(
        FilterFactory.and(TimeFilterApi.gt(1), TimeFilterApi.ltEq(1)),
        FilterFactory.or(TimeFilterApi.ltEq(1), TimeFilterApi.gt(1)).reverse());
    Assert.assertEquals(
        FilterFactory.or(TimeFilterApi.ltEq(1), TimeFilterApi.gt(1)),
        FilterFactory.and(TimeFilterApi.gt(1), TimeFilterApi.ltEq(1)).reverse());
  }

  @Test
  public void testRemoveNot() {
    Assert.assertEquals(
        TimeFilterApi.ltEq(1),
        PredicateRemoveNotRewriter.rewrite(FilterFactory.not(TimeFilterApi.gt(1))));
    Assert.assertEquals(
        ValueFilterApi.like("s*"),
        PredicateRemoveNotRewriter.rewrite(FilterFactory.not(ValueFilterApi.notLike("s*"))));
    Assert.assertEquals(
        FilterFactory.or(TimeFilterApi.gt(1), TimeFilterApi.ltEq(1)),
        PredicateRemoveNotRewriter.rewrite(
            FilterFactory.or(
                FilterFactory.not(TimeFilterApi.ltEq(1)), FilterFactory.not(TimeFilterApi.gt(1)))));
    Assert.assertEquals(
        FilterFactory.and(TimeFilterApi.gt(1), TimeFilterApi.ltEq(1)),
        PredicateRemoveNotRewriter.rewrite(
            FilterFactory.and(
                FilterFactory.not(TimeFilterApi.ltEq(1)), FilterFactory.not(TimeFilterApi.gt(1)))));
  }
}
