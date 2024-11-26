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

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.regex.Pattern;

import static org.apache.tsfile.enums.TSDataType.INT32;
import static org.apache.tsfile.read.filter.FilterTestUtil.DEFAULT_TIMESTAMP;
import static org.apache.tsfile.read.filter.factory.ValueFilterApi.DEFAULT_MEASUREMENT_INDEX;

public class NumericalOperatorsTest {
  @Test
  public void testEq() {
    Filter eq = ValueFilterApi.eq(DEFAULT_MEASUREMENT_INDEX, 10, INT32);
    Assert.assertTrue(eq.satisfyInteger(DEFAULT_TIMESTAMP, 10));
    Assert.assertFalse(eq.satisfyInteger(DEFAULT_TIMESTAMP, 20));
  }

  @Test
  public void testNotEq() {
    Filter notEq = ValueFilterApi.notEq(DEFAULT_MEASUREMENT_INDEX, 10, INT32);
    Assert.assertTrue(notEq.satisfyInteger(DEFAULT_TIMESTAMP, 20));
    Assert.assertFalse(notEq.satisfyInteger(DEFAULT_TIMESTAMP, 10));
  }

  @Test
  public void testGt() {
    Filter gt = ValueFilterApi.gt(DEFAULT_MEASUREMENT_INDEX, 10, INT32);
    Assert.assertTrue(gt.satisfyInteger(DEFAULT_TIMESTAMP, 20));
    Assert.assertFalse(gt.satisfyInteger(DEFAULT_TIMESTAMP, 10));
    Assert.assertFalse(gt.satisfyInteger(DEFAULT_TIMESTAMP, 0));
  }

  @Test
  public void testGtEq() {
    Filter gtEq = ValueFilterApi.gtEq(DEFAULT_MEASUREMENT_INDEX, 10, INT32);
    Assert.assertTrue(gtEq.satisfyInteger(DEFAULT_TIMESTAMP, 10));
    Assert.assertTrue(gtEq.satisfyInteger(DEFAULT_TIMESTAMP, 20));
    Assert.assertFalse(gtEq.satisfyInteger(DEFAULT_TIMESTAMP, 0));
  }

  @Test
  public void testLt() {
    Filter lt = ValueFilterApi.lt(DEFAULT_MEASUREMENT_INDEX, 10, INT32);
    Assert.assertTrue(lt.satisfyInteger(DEFAULT_TIMESTAMP, 0));
    Assert.assertFalse(lt.satisfyInteger(DEFAULT_TIMESTAMP, 10));
    Assert.assertFalse(lt.satisfyInteger(DEFAULT_TIMESTAMP, 20));
  }

  @Test
  public void testLtEq() {
    Filter ltEq = ValueFilterApi.ltEq(DEFAULT_MEASUREMENT_INDEX, 10, INT32);
    Assert.assertTrue(ltEq.satisfyInteger(DEFAULT_TIMESTAMP, 0));
    Assert.assertTrue(ltEq.satisfyInteger(DEFAULT_TIMESTAMP, 10));
    Assert.assertFalse(ltEq.satisfyInteger(DEFAULT_TIMESTAMP, 20));
  }

  @Test
  public void testBetweenAnd() {
    Filter between = ValueFilterApi.between(DEFAULT_MEASUREMENT_INDEX, 10, 20, INT32);
    Assert.assertTrue(between.satisfyInteger(DEFAULT_TIMESTAMP, 15));
    Assert.assertFalse(between.satisfyInteger(DEFAULT_TIMESTAMP, 30));
  }

  @Test
  public void testNotBetweenAnd() {
    Filter notBetween = ValueFilterApi.notBetween(DEFAULT_MEASUREMENT_INDEX, 10, 20, INT32);
    Assert.assertTrue(notBetween.satisfyInteger(DEFAULT_TIMESTAMP, 30));
    Assert.assertFalse(notBetween.satisfyInteger(DEFAULT_TIMESTAMP, 15));
  }

  @Test
  public void testIn() {
    Filter in =
        ValueFilterApi.in(
            DEFAULT_MEASUREMENT_INDEX, new HashSet<>(Arrays.asList(10, 20, 30)), INT32);
    Assert.assertTrue(in.satisfyInteger(DEFAULT_TIMESTAMP, 10));
    Assert.assertFalse(in.satisfyInteger(DEFAULT_TIMESTAMP, 0));

    Filter in2 =
        ValueFilterApi.in(
            DEFAULT_MEASUREMENT_INDEX, new HashSet<>(Arrays.asList(10, null, 30)), INT32);
    Assert.assertTrue(in2.satisfyInteger(DEFAULT_TIMESTAMP, 10));
  }

  @Test
  public void testNotIn() {
    Filter notIn =
        ValueFilterApi.notIn(
            DEFAULT_MEASUREMENT_INDEX, new HashSet<>(Arrays.asList(10, 20, 30)), INT32);
    Assert.assertTrue(notIn.satisfyInteger(DEFAULT_TIMESTAMP, 0));
    Assert.assertFalse(notIn.satisfyInteger(DEFAULT_TIMESTAMP, 10));
  }

  @Test
  public void testRegexp() {
    Filter regexp = ValueFilterApi.regexp(DEFAULT_MEASUREMENT_INDEX, Pattern.compile("1.*"), INT32);
    Assert.assertTrue(regexp.satisfyInteger(DEFAULT_TIMESTAMP, 10));
    Assert.assertFalse(regexp.satisfyInteger(DEFAULT_TIMESTAMP, 20));
  }

  @Test
  public void testNotRegexp() {
    Filter notRegexp =
        ValueFilterApi.notRegexp(DEFAULT_MEASUREMENT_INDEX, Pattern.compile("1.*"), INT32);
    Assert.assertTrue(notRegexp.satisfyInteger(DEFAULT_TIMESTAMP, 20));
    Assert.assertFalse(notRegexp.satisfyInteger(DEFAULT_TIMESTAMP, 10));
  }

  @Test
  public void testLike() {
    Filter regexp =
        ValueFilterApi.like(
            DEFAULT_MEASUREMENT_INDEX, LikePattern.compile("1%", Optional.empty()), INT32);
    Assert.assertTrue(regexp.satisfyInteger(DEFAULT_TIMESTAMP, 10));
    Assert.assertFalse(regexp.satisfyInteger(DEFAULT_TIMESTAMP, 20));
  }

  @Test
  public void testNotLike() {
    Filter notRegexp =
        ValueFilterApi.notLike(
            DEFAULT_MEASUREMENT_INDEX, LikePattern.compile("1%", Optional.empty()), INT32);
    Assert.assertTrue(notRegexp.satisfyInteger(DEFAULT_TIMESTAMP, 20));
    Assert.assertFalse(notRegexp.satisfyInteger(DEFAULT_TIMESTAMP, 10));
  }
}
