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
import org.apache.iotdb.tsfile.read.filter.factory.FilterType;
import org.apache.iotdb.tsfile.read.filter.operator.Between;
import org.apache.iotdb.tsfile.read.filter.operator.Eq;
import org.apache.iotdb.tsfile.read.filter.operator.Gt;
import org.apache.iotdb.tsfile.read.filter.operator.GtEq;
import org.apache.iotdb.tsfile.read.filter.operator.In;
import org.apache.iotdb.tsfile.read.filter.operator.Like;
import org.apache.iotdb.tsfile.read.filter.operator.Lt;
import org.apache.iotdb.tsfile.read.filter.operator.LtEq;
import org.apache.iotdb.tsfile.read.filter.operator.NotEq;
import org.apache.iotdb.tsfile.read.filter.operator.Regexp;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;

public class PredicateRemoveNotRewriterTest {

  @Test
  public void testReverse() {
    Assert.assertEquals(
        new Gt<>(1, FilterType.TIME_FILTER), new LtEq<>(1, FilterType.TIME_FILTER).reverse());
    Assert.assertEquals(
        new GtEq<>(1, FilterType.TIME_FILTER), new Lt<>(1, FilterType.TIME_FILTER).reverse());
    Assert.assertEquals(
        new Lt<>(1, FilterType.TIME_FILTER), new GtEq<>(1, FilterType.TIME_FILTER).reverse());
    Assert.assertEquals(
        new LtEq<>(1, FilterType.TIME_FILTER), new Gt<>(1, FilterType.TIME_FILTER).reverse());
    Assert.assertEquals(
        new Eq<>(1, FilterType.TIME_FILTER), new NotEq<>(1, FilterType.TIME_FILTER).reverse());
    Assert.assertEquals(
        new NotEq<>(1, FilterType.TIME_FILTER), new Eq<>(1, FilterType.TIME_FILTER).reverse());
    Assert.assertEquals(
        new Like<>("s*", FilterType.TIME_FILTER, true),
        new Like<>("s*", FilterType.TIME_FILTER, false).reverse());
    Assert.assertEquals(
        new Like<>("s*", FilterType.TIME_FILTER, false),
        new Like<>("s*", FilterType.TIME_FILTER, true).reverse());
    Assert.assertEquals(
        new Regexp<>("s*", FilterType.TIME_FILTER, true),
        new Regexp<>("s*", FilterType.TIME_FILTER, false).reverse());
    Assert.assertEquals(
        new Regexp<>("s*", FilterType.TIME_FILTER, false),
        new Regexp<>("s*", FilterType.TIME_FILTER, true).reverse());
    Assert.assertEquals(
        new Between<>(1, 100, FilterType.TIME_FILTER, true),
        new Between<>(1, 100, FilterType.TIME_FILTER, false).reverse());
    Assert.assertEquals(
        new Between<>(1, 100, FilterType.TIME_FILTER, false),
        new Between<>(1, 100, FilterType.TIME_FILTER, true).reverse());
    Assert.assertEquals(
        new In<>(new HashSet<>(Arrays.asList("a", "b")), FilterType.TIME_FILTER, true),
        new In<>(new HashSet<>(Arrays.asList("a", "b")), FilterType.TIME_FILTER, false).reverse());
    Assert.assertEquals(
        new In<>(new HashSet<>(Arrays.asList("a", "b")), FilterType.TIME_FILTER, false),
        new In<>(new HashSet<>(Arrays.asList("a", "b")), FilterType.TIME_FILTER, true).reverse());
    Assert.assertEquals(
        FilterFactory.not(new Gt<>(1, FilterType.TIME_FILTER)),
        new Gt<>(1, FilterType.TIME_FILTER).reverse());
    Assert.assertEquals(
        FilterFactory.and(
            new Gt<>(1, FilterType.TIME_FILTER), new LtEq<>(1, FilterType.TIME_FILTER)),
        FilterFactory.or(new LtEq<>(1, FilterType.TIME_FILTER), new Gt<>(1, FilterType.TIME_FILTER))
            .reverse());
    Assert.assertEquals(
        FilterFactory.or(
            new LtEq<>(1, FilterType.TIME_FILTER), new Gt<>(1, FilterType.TIME_FILTER)),
        FilterFactory.and(
                new Gt<>(1, FilterType.TIME_FILTER), new LtEq<>(1, FilterType.TIME_FILTER))
            .reverse());
  }

  @Test
  public void testRemoveNot() {
    Assert.assertEquals(
        new LtEq<>(1, FilterType.TIME_FILTER),
        PredicateRemoveNotRewriter.rewriter(
            FilterFactory.not(new Gt<>(1, FilterType.TIME_FILTER))));
    Assert.assertEquals(
        new Like<>("s*", FilterType.TIME_FILTER, false),
        PredicateRemoveNotRewriter.rewriter(
            FilterFactory.not(new Like<>("s*", FilterType.TIME_FILTER, true))));
    Assert.assertEquals(
        FilterFactory.or(
            new Gt<>(1, FilterType.TIME_FILTER), new LtEq<>(1, FilterType.TIME_FILTER)),
        PredicateRemoveNotRewriter.rewriter(
            FilterFactory.or(
                FilterFactory.not(new LtEq<>(1, FilterType.TIME_FILTER)),
                FilterFactory.not(new Gt<>(1, FilterType.TIME_FILTER)))));
    Assert.assertEquals(
        FilterFactory.and(
            new Gt<>(1, FilterType.TIME_FILTER), new LtEq<>(1, FilterType.TIME_FILTER)),
        PredicateRemoveNotRewriter.rewriter(
            FilterFactory.and(
                FilterFactory.not(new LtEq<>(1, FilterType.TIME_FILTER)),
                FilterFactory.not(new Gt<>(1, FilterType.TIME_FILTER)))));
  }
}
