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

import static org.apache.tsfile.enums.TSDataType.BOOLEAN;
import static org.apache.tsfile.read.filter.FilterTestUtil.DEFAULT_TIMESTAMP;
import static org.apache.tsfile.read.filter.factory.ValueFilterApi.DEFAULT_MEASUREMENT_INDEX;

public class BooleanOperatorsTest {
  @Test
  public void testEq() {
    Filter eq = ValueFilterApi.eq(DEFAULT_MEASUREMENT_INDEX, true, BOOLEAN);
    Assert.assertTrue(eq.satisfyBoolean(DEFAULT_TIMESTAMP, true));
    Assert.assertFalse(eq.satisfyBoolean(DEFAULT_TIMESTAMP, false));
  }

  @Test
  public void testNotEq() {
    Filter notEq = ValueFilterApi.notEq(DEFAULT_MEASUREMENT_INDEX, true, BOOLEAN);
    Assert.assertTrue(notEq.satisfyBoolean(DEFAULT_TIMESTAMP, false));
    Assert.assertFalse(notEq.satisfyBoolean(DEFAULT_TIMESTAMP, true));
  }

  @Test
  public void testGt() {
    Filter gt = ValueFilterApi.gt(DEFAULT_MEASUREMENT_INDEX, true, BOOLEAN);
    Assert.assertFalse(gt.satisfyBoolean(DEFAULT_TIMESTAMP, true));
    Assert.assertFalse(gt.satisfyBoolean(DEFAULT_TIMESTAMP, false));
  }

  @Test
  public void testGtEq() {
    Filter gtEq = ValueFilterApi.gtEq(DEFAULT_MEASUREMENT_INDEX, true, BOOLEAN);
    Assert.assertTrue(gtEq.satisfyBoolean(DEFAULT_TIMESTAMP, true));
    Assert.assertFalse(gtEq.satisfyBoolean(DEFAULT_TIMESTAMP, false));
  }

  @Test
  public void testLt() {
    Filter lt = ValueFilterApi.lt(DEFAULT_MEASUREMENT_INDEX, true, BOOLEAN);
    Assert.assertTrue(lt.satisfyBoolean(DEFAULT_TIMESTAMP, false));
    Assert.assertFalse(lt.satisfyBoolean(DEFAULT_TIMESTAMP, true));
  }

  @Test
  public void testLtEq() {
    Filter ltEq = ValueFilterApi.ltEq(DEFAULT_MEASUREMENT_INDEX, true, BOOLEAN);
    Assert.assertTrue(ltEq.satisfyBoolean(DEFAULT_TIMESTAMP, true));
    Assert.assertTrue(ltEq.satisfyBoolean(DEFAULT_TIMESTAMP, false));
  }

  @Test
  public void testBetweenAnd() {
    Filter between = ValueFilterApi.between(DEFAULT_MEASUREMENT_INDEX, false, true, BOOLEAN);
    Assert.assertTrue(between.satisfyBoolean(DEFAULT_TIMESTAMP, false));
    Assert.assertTrue(between.satisfyBoolean(DEFAULT_TIMESTAMP, true));

    Filter between2 = ValueFilterApi.between(DEFAULT_MEASUREMENT_INDEX, true, true, BOOLEAN);
    Assert.assertFalse(between2.satisfyBoolean(DEFAULT_TIMESTAMP, false));
    Assert.assertTrue(between2.satisfyBoolean(DEFAULT_TIMESTAMP, true));
  }

  @Test
  public void testNotBetweenAnd() {
    Filter notBetween = ValueFilterApi.notBetween(DEFAULT_MEASUREMENT_INDEX, false, true, BOOLEAN);
    Assert.assertFalse(notBetween.satisfyBoolean(DEFAULT_TIMESTAMP, false));
    Assert.assertFalse(notBetween.satisfyBoolean(DEFAULT_TIMESTAMP, true));

    Filter notBetween2 = ValueFilterApi.notBetween(DEFAULT_MEASUREMENT_INDEX, true, true, BOOLEAN);
    Assert.assertTrue(notBetween2.satisfyBoolean(DEFAULT_TIMESTAMP, false));
    Assert.assertFalse(notBetween2.satisfyBoolean(DEFAULT_TIMESTAMP, true));
  }

  @Test
  public void testIn() {
    Filter in =
        ValueFilterApi.in(
            DEFAULT_MEASUREMENT_INDEX, new HashSet<>(Arrays.asList(true, false)), BOOLEAN);
    Assert.assertTrue(in.satisfyBoolean(DEFAULT_TIMESTAMP, true));
    Assert.assertTrue(in.satisfyBoolean(DEFAULT_TIMESTAMP, false));

    Filter in2 =
        ValueFilterApi.in(DEFAULT_MEASUREMENT_INDEX, new HashSet<>(Arrays.asList(true)), BOOLEAN);
    Assert.assertTrue(in2.satisfyBoolean(DEFAULT_TIMESTAMP, true));
    Assert.assertFalse(in2.satisfyBoolean(DEFAULT_TIMESTAMP, false));
  }

  @Test
  public void testNotIn() {
    Filter notIn =
        ValueFilterApi.notIn(
            DEFAULT_MEASUREMENT_INDEX, new HashSet<>(Arrays.asList(true, false)), BOOLEAN);
    Assert.assertFalse(notIn.satisfyBoolean(DEFAULT_TIMESTAMP, true));
    Assert.assertFalse(notIn.satisfyBoolean(DEFAULT_TIMESTAMP, false));

    Filter notIn2 =
        ValueFilterApi.notIn(
            DEFAULT_MEASUREMENT_INDEX, new HashSet<>(Arrays.asList(true)), BOOLEAN);
    Assert.assertFalse(notIn2.satisfyBoolean(DEFAULT_TIMESTAMP, true));
    Assert.assertTrue(notIn2.satisfyBoolean(DEFAULT_TIMESTAMP, false));
  }

  @Test
  public void testRegexp() {
    Filter regexp =
        ValueFilterApi.regexp(DEFAULT_MEASUREMENT_INDEX, Pattern.compile("t.*"), BOOLEAN);
    Assert.assertTrue(regexp.satisfyBoolean(DEFAULT_TIMESTAMP, true));
    Assert.assertFalse(regexp.satisfyBoolean(DEFAULT_TIMESTAMP, false));
  }

  @Test
  public void testNotRegexp() {
    Filter notRegexp =
        ValueFilterApi.notRegexp(DEFAULT_MEASUREMENT_INDEX, Pattern.compile("t.*"), BOOLEAN);
    Assert.assertTrue(notRegexp.satisfyBoolean(DEFAULT_TIMESTAMP, false));
    Assert.assertFalse(notRegexp.satisfyBoolean(DEFAULT_TIMESTAMP, true));
  }

  @Test
  public void testLike() {
    Filter regexp =
        ValueFilterApi.like(
            DEFAULT_MEASUREMENT_INDEX, LikePattern.compile("t%", Optional.empty()), BOOLEAN);
    Assert.assertTrue(regexp.satisfyBoolean(DEFAULT_TIMESTAMP, true));
    Assert.assertFalse(regexp.satisfyBoolean(DEFAULT_TIMESTAMP, false));
  }

  @Test
  public void testNotLike() {
    Filter notRegexp =
        ValueFilterApi.notLike(
            DEFAULT_MEASUREMENT_INDEX, LikePattern.compile("t%", Optional.empty()), BOOLEAN);
    Assert.assertTrue(notRegexp.satisfyBoolean(DEFAULT_TIMESTAMP, false));
    Assert.assertFalse(notRegexp.satisfyBoolean(DEFAULT_TIMESTAMP, true));
  }
}
