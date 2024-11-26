/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tsfile.read.filter;

import org.apache.tsfile.common.regexp.LikePattern;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.factory.ValueFilterApi;
import org.apache.tsfile.utils.Binary;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.regex.Pattern;

import static org.apache.tsfile.common.conf.TSFileConfig.STRING_CHARSET;
import static org.apache.tsfile.enums.TSDataType.TEXT;
import static org.apache.tsfile.read.filter.FilterTestUtil.DEFAULT_TIMESTAMP;
import static org.apache.tsfile.read.filter.factory.ValueFilterApi.DEFAULT_MEASUREMENT_INDEX;

public class BinaryOperatorsTest {

  @Test
  public void testEq() {
    Filter eq = ValueFilterApi.eq(DEFAULT_MEASUREMENT_INDEX, new Binary("a", STRING_CHARSET), TEXT);
    Assert.assertTrue(eq.satisfyBinary(DEFAULT_TIMESTAMP, new Binary("a", STRING_CHARSET)));
    Assert.assertFalse(eq.satisfyBinary(DEFAULT_TIMESTAMP, new Binary("b", STRING_CHARSET)));
  }

  @Test
  public void testNotEq() {
    Filter notEq =
        ValueFilterApi.notEq(DEFAULT_MEASUREMENT_INDEX, new Binary("a", STRING_CHARSET), TEXT);
    Assert.assertTrue(notEq.satisfyBinary(DEFAULT_TIMESTAMP, new Binary("b", STRING_CHARSET)));
    Assert.assertFalse(notEq.satisfyBinary(DEFAULT_TIMESTAMP, new Binary("a", STRING_CHARSET)));
  }

  @Test
  public void testGt() {
    Filter gt = ValueFilterApi.gt(DEFAULT_MEASUREMENT_INDEX, new Binary("a", STRING_CHARSET), TEXT);
    Assert.assertTrue(gt.satisfyBinary(DEFAULT_TIMESTAMP, new Binary("b", STRING_CHARSET)));
    Assert.assertFalse(gt.satisfyBinary(DEFAULT_TIMESTAMP, new Binary("a", STRING_CHARSET)));
  }

  @Test
  public void testGtEq() {
    Filter gtEq =
        ValueFilterApi.gtEq(DEFAULT_MEASUREMENT_INDEX, new Binary("a", STRING_CHARSET), TEXT);
    Assert.assertTrue(gtEq.satisfyBinary(DEFAULT_TIMESTAMP, new Binary("a", STRING_CHARSET)));
    Assert.assertTrue(gtEq.satisfyBinary(DEFAULT_TIMESTAMP, new Binary("b", STRING_CHARSET)));
    Assert.assertTrue(gtEq.satisfyBinary(DEFAULT_TIMESTAMP, new Binary("c", STRING_CHARSET)));
  }

  @Test
  public void testLt() {
    Filter lt = ValueFilterApi.lt(DEFAULT_MEASUREMENT_INDEX, new Binary("b", STRING_CHARSET), TEXT);
    Assert.assertTrue(lt.satisfyBinary(DEFAULT_TIMESTAMP, new Binary("a", STRING_CHARSET)));
    Assert.assertFalse(lt.satisfyBinary(DEFAULT_TIMESTAMP, new Binary("b", STRING_CHARSET)));
  }

  @Test
  public void testLtEq() {
    Filter ltEq =
        ValueFilterApi.ltEq(DEFAULT_MEASUREMENT_INDEX, new Binary("b", STRING_CHARSET), TEXT);
    Assert.assertTrue(ltEq.satisfyBinary(DEFAULT_TIMESTAMP, new Binary("a", STRING_CHARSET)));
    Assert.assertTrue(ltEq.satisfyBinary(DEFAULT_TIMESTAMP, new Binary("b", STRING_CHARSET)));
    Assert.assertFalse(ltEq.satisfyBinary(DEFAULT_TIMESTAMP, new Binary("c", STRING_CHARSET)));
  }

  @Test
  public void testBetweenAnd() {
    Filter between =
        ValueFilterApi.between(
            DEFAULT_MEASUREMENT_INDEX,
            new Binary("a", STRING_CHARSET),
            new Binary("c", STRING_CHARSET),
            TEXT);
    Assert.assertTrue(between.satisfyBinary(DEFAULT_TIMESTAMP, new Binary("b", STRING_CHARSET)));
    Assert.assertFalse(between.satisfyBinary(DEFAULT_TIMESTAMP, new Binary("d", STRING_CHARSET)));
  }

  @Test
  public void testNotBetweenAnd() {
    Filter notBetween =
        ValueFilterApi.notBetween(
            DEFAULT_MEASUREMENT_INDEX,
            new Binary("a", STRING_CHARSET),
            new Binary("c", STRING_CHARSET),
            TEXT);
    Assert.assertTrue(notBetween.satisfyBinary(DEFAULT_TIMESTAMP, new Binary("d", STRING_CHARSET)));
    Assert.assertFalse(
        notBetween.satisfyBinary(DEFAULT_TIMESTAMP, new Binary("b", STRING_CHARSET)));
  }

  @Test
  public void testIn() {
    Filter in =
        ValueFilterApi.in(
            DEFAULT_MEASUREMENT_INDEX,
            new HashSet<>(
                Arrays.asList(new Binary("a", STRING_CHARSET), new Binary("b", STRING_CHARSET))),
            TEXT);
    Assert.assertTrue(in.satisfyBinary(DEFAULT_TIMESTAMP, new Binary("a", STRING_CHARSET)));
    Assert.assertFalse(in.satisfyBinary(DEFAULT_TIMESTAMP, new Binary("c", STRING_CHARSET)));
  }

  @Test
  public void testNotIn() {
    Filter in =
        ValueFilterApi.notIn(
            DEFAULT_MEASUREMENT_INDEX,
            new HashSet<>(
                Arrays.asList(new Binary("a", STRING_CHARSET), new Binary("b", STRING_CHARSET))),
            TEXT);
    Assert.assertTrue(in.satisfyBinary(DEFAULT_TIMESTAMP, new Binary("c", STRING_CHARSET)));
    Assert.assertFalse(in.satisfyBinary(DEFAULT_TIMESTAMP, new Binary("a", STRING_CHARSET)));
  }

  @Test
  public void testRegexp() {
    Filter regexp = ValueFilterApi.regexp(DEFAULT_MEASUREMENT_INDEX, Pattern.compile("a.*"), TEXT);
    Assert.assertTrue(regexp.satisfyBinary(DEFAULT_TIMESTAMP, new Binary("abc", STRING_CHARSET)));
    Assert.assertFalse(regexp.satisfyBinary(DEFAULT_TIMESTAMP, new Binary("bcd", STRING_CHARSET)));
  }

  @Test
  public void testNotRegexp() {
    Filter notRegexp =
        ValueFilterApi.notRegexp(DEFAULT_MEASUREMENT_INDEX, Pattern.compile("a.*"), TEXT);
    Assert.assertTrue(
        notRegexp.satisfyBinary(DEFAULT_TIMESTAMP, new Binary("bcd", STRING_CHARSET)));
    Assert.assertFalse(
        notRegexp.satisfyBinary(DEFAULT_TIMESTAMP, new Binary("abc", STRING_CHARSET)));
  }

  @Test
  public void testLike() {
    Filter regexp =
        ValueFilterApi.like(
            DEFAULT_MEASUREMENT_INDEX, LikePattern.compile("a%", Optional.empty()), TEXT);
    Assert.assertTrue(regexp.satisfyBinary(DEFAULT_TIMESTAMP, new Binary("abc", STRING_CHARSET)));
    Assert.assertFalse(regexp.satisfyBinary(DEFAULT_TIMESTAMP, new Binary("bcd", STRING_CHARSET)));
  }

  @Test
  public void testNotLike() {
    Filter notRegexp =
        ValueFilterApi.notLike(
            DEFAULT_MEASUREMENT_INDEX, LikePattern.compile("a%", Optional.empty()), TEXT);
    Assert.assertTrue(
        notRegexp.satisfyBinary(DEFAULT_TIMESTAMP, new Binary("bcd", STRING_CHARSET)));
    Assert.assertFalse(
        notRegexp.satisfyBinary(DEFAULT_TIMESTAMP, new Binary("abc", STRING_CHARSET)));
  }

  @Test
  public void testStatistics() {}
}
