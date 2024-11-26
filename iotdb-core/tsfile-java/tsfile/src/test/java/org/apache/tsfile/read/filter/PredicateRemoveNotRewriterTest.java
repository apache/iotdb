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

package org.apache.tsfile.read.filter;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.regexp.LikePattern;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.filter.factory.FilterFactory;
import org.apache.tsfile.read.filter.factory.TimeFilterApi;
import org.apache.tsfile.read.filter.factory.ValueFilterApi;
import org.apache.tsfile.utils.Binary;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.regex.Pattern;

import static org.apache.tsfile.read.filter.factory.ValueFilterApi.DEFAULT_MEASUREMENT_INDEX;

public class PredicateRemoveNotRewriterTest {

  @Test
  public void testReverse() {
    Assert.assertEquals(TimeFilterApi.gt(1), TimeFilterApi.ltEq(1).reverse());
    Assert.assertEquals(TimeFilterApi.gtEq(1), TimeFilterApi.lt(1).reverse());
    Assert.assertEquals(TimeFilterApi.lt(1), TimeFilterApi.gtEq(1).reverse());
    Assert.assertEquals(TimeFilterApi.ltEq(1), TimeFilterApi.gt(1).reverse());
    Assert.assertEquals(TimeFilterApi.eq(1), TimeFilterApi.notEq(1).reverse());
    Assert.assertEquals(TimeFilterApi.notEq(1), TimeFilterApi.eq(1).reverse());
    Assert.assertEquals(
        ValueFilterApi.like(
            DEFAULT_MEASUREMENT_INDEX,
            LikePattern.compile("s%", Optional.empty()),
            TSDataType.TEXT),
        ValueFilterApi.notLike(
                DEFAULT_MEASUREMENT_INDEX,
                LikePattern.compile("s%", Optional.empty()),
                TSDataType.TEXT)
            .reverse());
    Assert.assertEquals(
        ValueFilterApi.notLike(
            DEFAULT_MEASUREMENT_INDEX,
            LikePattern.compile("s%", Optional.empty()),
            TSDataType.TEXT),
        ValueFilterApi.like(
                DEFAULT_MEASUREMENT_INDEX,
                LikePattern.compile("s%", Optional.empty()),
                TSDataType.TEXT)
            .reverse());
    Assert.assertEquals(
        ValueFilterApi.regexp(DEFAULT_MEASUREMENT_INDEX, Pattern.compile("s*"), TSDataType.TEXT),
        ValueFilterApi.notRegexp(DEFAULT_MEASUREMENT_INDEX, Pattern.compile("s*"), TSDataType.TEXT)
            .reverse());
    Assert.assertEquals(
        ValueFilterApi.notRegexp(DEFAULT_MEASUREMENT_INDEX, Pattern.compile("s*"), TSDataType.TEXT),
        ValueFilterApi.regexp(DEFAULT_MEASUREMENT_INDEX, Pattern.compile("s*"), TSDataType.TEXT)
            .reverse());
    Assert.assertEquals(TimeFilterApi.between(1, 100), TimeFilterApi.notBetween(1, 100).reverse());
    Assert.assertEquals(TimeFilterApi.notBetween(1, 100), TimeFilterApi.between(1, 100).reverse());
    // TODO: add StringFilter test
    Assert.assertEquals(
        ValueFilterApi.in(
            DEFAULT_MEASUREMENT_INDEX,
            new HashSet<>(
                Arrays.asList(
                    new Binary("a", TSFileConfig.STRING_CHARSET),
                    new Binary("b", TSFileConfig.STRING_CHARSET))),
            TSDataType.TEXT),
        ValueFilterApi.notIn(
                DEFAULT_MEASUREMENT_INDEX,
                new HashSet<>(
                    Arrays.asList(
                        new Binary("a", TSFileConfig.STRING_CHARSET),
                        new Binary("b", TSFileConfig.STRING_CHARSET))),
                TSDataType.TEXT)
            .reverse());
    Assert.assertEquals(
        ValueFilterApi.in(
            DEFAULT_MEASUREMENT_INDEX,
            new HashSet<>(
                Arrays.asList(
                    new Binary("a", TSFileConfig.STRING_CHARSET),
                    new Binary("b", TSFileConfig.STRING_CHARSET))),
            TSDataType.TEXT),
        ValueFilterApi.notIn(
                DEFAULT_MEASUREMENT_INDEX,
                new HashSet<>(
                    Arrays.asList(
                        new Binary("a", TSFileConfig.STRING_CHARSET),
                        new Binary("b", TSFileConfig.STRING_CHARSET))),
                TSDataType.TEXT)
            .reverse());
    Assert.assertEquals(
        ValueFilterApi.notIn(
            DEFAULT_MEASUREMENT_INDEX,
            new HashSet<>(
                Arrays.asList(
                    new Binary("a", TSFileConfig.STRING_CHARSET),
                    new Binary("b", TSFileConfig.STRING_CHARSET))),
            TSDataType.TEXT),
        ValueFilterApi.in(
                DEFAULT_MEASUREMENT_INDEX,
                new HashSet<>(
                    Arrays.asList(
                        new Binary("a", TSFileConfig.STRING_CHARSET),
                        new Binary("b", TSFileConfig.STRING_CHARSET))),
                TSDataType.TEXT)
            .reverse());
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
        ValueFilterApi.regexp(DEFAULT_MEASUREMENT_INDEX, Pattern.compile("s*"), TSDataType.TEXT),
        PredicateRemoveNotRewriter.rewrite(
            FilterFactory.not(
                ValueFilterApi.notRegexp(
                    DEFAULT_MEASUREMENT_INDEX, Pattern.compile("s*"), TSDataType.TEXT))));
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
