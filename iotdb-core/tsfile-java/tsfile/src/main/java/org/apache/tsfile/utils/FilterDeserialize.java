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

package org.apache.tsfile.utils;

import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.basic.OperatorType;
import org.apache.tsfile.read.filter.operator.BinaryFilterOperators;
import org.apache.tsfile.read.filter.operator.BooleanFilterOperators;
import org.apache.tsfile.read.filter.operator.DoubleFilterOperators;
import org.apache.tsfile.read.filter.operator.FloatFilterOperators;
import org.apache.tsfile.read.filter.operator.IntegerFilterOperators;
import org.apache.tsfile.read.filter.operator.LongFilterOperators;
import org.apache.tsfile.read.filter.operator.StringFilterOperators;
import org.apache.tsfile.read.filter.operator.ValueIsNotNullOperator;
import org.apache.tsfile.read.filter.operator.ValueIsNullOperator;
import org.apache.tsfile.utils.ReadWriteIOUtils.ClassSerializeId;

import java.nio.ByteBuffer;

public class FilterDeserialize {

  private static final String UNSUPPORTED_DATATYPE_MESSAGE = "Unsupported class serialize id:";

  public static Filter deserializeValueFilter(OperatorType type, ByteBuffer buffer) {
    ClassSerializeId classSerializeId = ClassSerializeId.values()[ReadWriteIOUtils.readInt(buffer)];
    switch (type) {
      case VALUE_EQ:
        return FilterDeserialize.deserializeValueEqFilter(classSerializeId, buffer);
      case VALUE_NEQ:
        return FilterDeserialize.deserializeValueNotEqFilter(classSerializeId, buffer);
      case VALUE_GT:
        return FilterDeserialize.deserializeValueGtFilter(classSerializeId, buffer);
      case VALUE_GTEQ:
        return FilterDeserialize.deserializeValueGtEqFilter(classSerializeId, buffer);
      case VALUE_LT:
        return FilterDeserialize.deserializeValueLtFilter(classSerializeId, buffer);
      case VALUE_LTEQ:
        return FilterDeserialize.deserializeValueLtEqFilter(classSerializeId, buffer);
      case VALUE_IS_NULL:
        return FilterDeserialize.deserializeValueIsNullFilter(buffer);
      case VALUE_IS_NOT_NULL:
        return FilterDeserialize.deserializeValueIsNotNullFilter(buffer);
      case VALUE_IN:
        return FilterDeserialize.deserializeValueInFilter(classSerializeId, buffer);
      case VALUE_NOT_IN:
        return FilterDeserialize.deserializeValueNotInFilter(classSerializeId, buffer);
      case VALUE_REGEXP:
        return FilterDeserialize.deserializeValueRegexpFilter(classSerializeId, buffer);
      case VALUE_NOT_REGEXP:
        return FilterDeserialize.deserializeValueNotRegexpFilter(classSerializeId, buffer);
      case VALUE_LIKE:
        return FilterDeserialize.deserializeValueLikeFilter(classSerializeId, buffer);
      case VALUE_NOT_LIKE:
        return FilterDeserialize.deserializeValueNotLikeFilter(classSerializeId, buffer);
      case VALUE_BETWEEN_AND:
        return FilterDeserialize.deserializeValueBetweenAndFilter(classSerializeId, buffer);
      case VALUE_NOT_BETWEEN_AND:
        return FilterDeserialize.deserializeValueNotBetweenAndFilter(classSerializeId, buffer);
      default:
        throw new UnsupportedOperationException("Unsupported operator type:" + type);
    }
  }

  public static Filter deserializeValueEqFilter(
      ClassSerializeId classSerializeId, ByteBuffer buffer) {
    switch (classSerializeId) {
      case BOOLEAN:
        return new BooleanFilterOperators.ValueEq(buffer);
      case INTEGER:
        return new IntegerFilterOperators.ValueEq(buffer);
      case LONG:
        return new LongFilterOperators.ValueEq(buffer);
      case FLOAT:
        return new FloatFilterOperators.ValueEq(buffer);
      case DOUBLE:
        return new DoubleFilterOperators.ValueEq(buffer);
      case BINARY:
        return new BinaryFilterOperators.ValueEq(buffer);
      case STRING:
        return new StringFilterOperators.ValueEq(buffer);
      default:
        throw new UnsupportedOperationException(UNSUPPORTED_DATATYPE_MESSAGE + classSerializeId);
    }
  }

  public static Filter deserializeValueNotEqFilter(
      ClassSerializeId classSerializeId, ByteBuffer buffer) {
    switch (classSerializeId) {
      case BOOLEAN:
        return new BooleanFilterOperators.ValueNotEq(buffer);
      case INTEGER:
        return new IntegerFilterOperators.ValueNotEq(buffer);
      case LONG:
        return new LongFilterOperators.ValueNotEq(buffer);
      case FLOAT:
        return new FloatFilterOperators.ValueNotEq(buffer);
      case DOUBLE:
        return new DoubleFilterOperators.ValueNotEq(buffer);
      case BINARY:
        return new BinaryFilterOperators.ValueNotEq(buffer);
      case STRING:
        return new StringFilterOperators.ValueNotEq(buffer);
      default:
        throw new UnsupportedOperationException(UNSUPPORTED_DATATYPE_MESSAGE + classSerializeId);
    }
  }

  public static Filter deserializeValueGtFilter(
      ClassSerializeId classSerializeId, ByteBuffer buffer) {
    switch (classSerializeId) {
      case BOOLEAN:
        return new BooleanFilterOperators.ValueGt(buffer);
      case INTEGER:
        return new IntegerFilterOperators.ValueGt(buffer);
      case LONG:
        return new LongFilterOperators.ValueGt(buffer);
      case FLOAT:
        return new FloatFilterOperators.ValueGt(buffer);
      case DOUBLE:
        return new DoubleFilterOperators.ValueGt(buffer);
      case BINARY:
        return new BinaryFilterOperators.ValueGt(buffer);
      case STRING:
        return new StringFilterOperators.ValueGt(buffer);
      default:
        throw new UnsupportedOperationException(UNSUPPORTED_DATATYPE_MESSAGE + classSerializeId);
    }
  }

  public static Filter deserializeValueGtEqFilter(
      ClassSerializeId classSerializeId, ByteBuffer buffer) {
    switch (classSerializeId) {
      case BOOLEAN:
        return new BooleanFilterOperators.ValueGtEq(buffer);
      case INTEGER:
        return new IntegerFilterOperators.ValueGtEq(buffer);
      case LONG:
        return new LongFilterOperators.ValueGtEq(buffer);
      case FLOAT:
        return new FloatFilterOperators.ValueGtEq(buffer);
      case DOUBLE:
        return new DoubleFilterOperators.ValueGtEq(buffer);
      case BINARY:
        return new BinaryFilterOperators.ValueGtEq(buffer);
      case STRING:
        return new StringFilterOperators.ValueGtEq(buffer);
      default:
        throw new UnsupportedOperationException(UNSUPPORTED_DATATYPE_MESSAGE + classSerializeId);
    }
  }

  public static Filter deserializeValueLtFilter(
      ClassSerializeId classSerializeId, ByteBuffer buffer) {
    switch (classSerializeId) {
      case BOOLEAN:
        return new BooleanFilterOperators.ValueLt(buffer);
      case INTEGER:
        return new IntegerFilterOperators.ValueLt(buffer);
      case LONG:
        return new LongFilterOperators.ValueLt(buffer);
      case FLOAT:
        return new FloatFilterOperators.ValueLt(buffer);
      case DOUBLE:
        return new DoubleFilterOperators.ValueLt(buffer);
      case BINARY:
        return new BinaryFilterOperators.ValueLt(buffer);
      case STRING:
        return new StringFilterOperators.ValueLt(buffer);
      default:
        throw new UnsupportedOperationException(UNSUPPORTED_DATATYPE_MESSAGE + classSerializeId);
    }
  }

  public static Filter deserializeValueLtEqFilter(
      ClassSerializeId classSerializeId, ByteBuffer buffer) {
    switch (classSerializeId) {
      case BOOLEAN:
        return new BooleanFilterOperators.ValueLtEq(buffer);
      case INTEGER:
        return new IntegerFilterOperators.ValueLtEq(buffer);
      case LONG:
        return new LongFilterOperators.ValueLtEq(buffer);
      case FLOAT:
        return new FloatFilterOperators.ValueLtEq(buffer);
      case DOUBLE:
        return new DoubleFilterOperators.ValueLtEq(buffer);
      case BINARY:
        return new BinaryFilterOperators.ValueLtEq(buffer);
      case STRING:
        return new StringFilterOperators.ValueLtEq(buffer);
      default:
        throw new UnsupportedOperationException(UNSUPPORTED_DATATYPE_MESSAGE + classSerializeId);
    }
  }

  public static Filter deserializeValueInFilter(
      ClassSerializeId classSerializeId, ByteBuffer buffer) {

    switch (classSerializeId) {
      case BOOLEAN:
        return new BooleanFilterOperators.ValueIn(buffer);
      case INTEGER:
        return new IntegerFilterOperators.ValueIn(buffer);
      case LONG:
        return new LongFilterOperators.ValueIn(buffer);
      case FLOAT:
        return new FloatFilterOperators.ValueIn(buffer);
      case DOUBLE:
        return new DoubleFilterOperators.ValueIn(buffer);
      case BINARY:
        return new BinaryFilterOperators.ValueIn(buffer);
      case STRING:
        return new StringFilterOperators.ValueIn(buffer);
      default:
        throw new UnsupportedOperationException(UNSUPPORTED_DATATYPE_MESSAGE + classSerializeId);
    }
  }

  public static Filter deserializeValueNotInFilter(
      ClassSerializeId classSerializeId, ByteBuffer buffer) {
    switch (classSerializeId) {
      case BOOLEAN:
        return new BooleanFilterOperators.ValueNotIn(buffer);
      case INTEGER:
        return new IntegerFilterOperators.ValueNotIn(buffer);
      case LONG:
        return new LongFilterOperators.ValueNotIn(buffer);
      case FLOAT:
        return new FloatFilterOperators.ValueNotIn(buffer);
      case DOUBLE:
        return new DoubleFilterOperators.ValueNotIn(buffer);
      case BINARY:
        return new BinaryFilterOperators.ValueNotIn(buffer);
      case STRING:
        return new StringFilterOperators.ValueNotIn(buffer);
      default:
        throw new UnsupportedOperationException(UNSUPPORTED_DATATYPE_MESSAGE + classSerializeId);
    }
  }

  public static Filter deserializeValueBetweenAndFilter(
      ClassSerializeId classSerializeId, ByteBuffer buffer) {
    switch (classSerializeId) {
      case BOOLEAN:
        return new BooleanFilterOperators.ValueBetweenAnd(buffer);
      case INTEGER:
        return new IntegerFilterOperators.ValueBetweenAnd(buffer);
      case LONG:
        return new LongFilterOperators.ValueBetweenAnd(buffer);
      case FLOAT:
        return new FloatFilterOperators.ValueBetweenAnd(buffer);
      case DOUBLE:
        return new DoubleFilterOperators.ValueBetweenAnd(buffer);
      case BINARY:
        return new BinaryFilterOperators.ValueBetweenAnd(buffer);
      case STRING:
        return new StringFilterOperators.ValueBetweenAnd(buffer);
      default:
        throw new UnsupportedOperationException(UNSUPPORTED_DATATYPE_MESSAGE + classSerializeId);
    }
  }

  public static Filter deserializeValueNotBetweenAndFilter(
      ClassSerializeId classSerializeId, ByteBuffer buffer) {
    switch (classSerializeId) {
      case BOOLEAN:
        return new BooleanFilterOperators.ValueNotBetweenAnd(buffer);
      case INTEGER:
        return new IntegerFilterOperators.ValueNotBetweenAnd(buffer);
      case LONG:
        return new LongFilterOperators.ValueNotBetweenAnd(buffer);
      case FLOAT:
        return new FloatFilterOperators.ValueNotBetweenAnd(buffer);
      case DOUBLE:
        return new DoubleFilterOperators.ValueNotBetweenAnd(buffer);
      case BINARY:
        return new BinaryFilterOperators.ValueNotBetweenAnd(buffer);
      case STRING:
        return new StringFilterOperators.ValueNotBetweenAnd(buffer);
      default:
        throw new UnsupportedOperationException(UNSUPPORTED_DATATYPE_MESSAGE + classSerializeId);
    }
  }

  public static Filter deserializeValueRegexpFilter(
      ClassSerializeId classSerializeId, ByteBuffer buffer) {
    switch (classSerializeId) {
      case BOOLEAN:
        return new BooleanFilterOperators.ValueRegexp(buffer);
      case INTEGER:
        return new IntegerFilterOperators.ValueRegexp(buffer);
      case LONG:
        return new LongFilterOperators.ValueRegexp(buffer);
      case FLOAT:
        return new FloatFilterOperators.ValueRegexp(buffer);
      case DOUBLE:
        return new DoubleFilterOperators.ValueRegexp(buffer);
      case BINARY:
        return new BinaryFilterOperators.ValueRegexp(buffer);
      case STRING:
        return new StringFilterOperators.ValueRegexp(buffer);
      default:
        throw new UnsupportedOperationException(UNSUPPORTED_DATATYPE_MESSAGE + classSerializeId);
    }
  }

  public static Filter deserializeValueNotRegexpFilter(
      ClassSerializeId classSerializeId, ByteBuffer buffer) {
    switch (classSerializeId) {
      case BOOLEAN:
        return new BooleanFilterOperators.ValueNotRegexp(buffer);
      case INTEGER:
        return new IntegerFilterOperators.ValueNotRegexp(buffer);
      case LONG:
        return new LongFilterOperators.ValueNotRegexp(buffer);
      case FLOAT:
        return new FloatFilterOperators.ValueNotRegexp(buffer);
      case DOUBLE:
        return new DoubleFilterOperators.ValueNotRegexp(buffer);
      case BINARY:
        return new BinaryFilterOperators.ValueNotRegexp(buffer);
      case STRING:
        return new StringFilterOperators.ValueNotRegexp(buffer);
      default:
        throw new UnsupportedOperationException(UNSUPPORTED_DATATYPE_MESSAGE + classSerializeId);
    }
  }

  public static Filter deserializeValueLikeFilter(
      ClassSerializeId classSerializeId, ByteBuffer buffer) {
    switch (classSerializeId) {
      case BOOLEAN:
        return new BooleanFilterOperators.ValueLike(buffer);
      case INTEGER:
        return new IntegerFilterOperators.ValueLike(buffer);
      case LONG:
        return new LongFilterOperators.ValueLike(buffer);
      case FLOAT:
        return new FloatFilterOperators.ValueLike(buffer);
      case DOUBLE:
        return new DoubleFilterOperators.ValueLike(buffer);
      case BINARY:
        return new BinaryFilterOperators.ValueLike(buffer);
      case STRING:
        return new StringFilterOperators.ValueLike(buffer);
      default:
        throw new UnsupportedOperationException(UNSUPPORTED_DATATYPE_MESSAGE + classSerializeId);
    }
  }

  public static Filter deserializeValueNotLikeFilter(
      ClassSerializeId classSerializeId, ByteBuffer buffer) {
    switch (classSerializeId) {
      case BOOLEAN:
        return new BooleanFilterOperators.ValueNotLike(buffer);
      case INTEGER:
        return new IntegerFilterOperators.ValueNotLike(buffer);
      case LONG:
        return new LongFilterOperators.ValueNotLike(buffer);
      case FLOAT:
        return new FloatFilterOperators.ValueNotLike(buffer);
      case DOUBLE:
        return new DoubleFilterOperators.ValueNotLike(buffer);
      case BINARY:
        return new BinaryFilterOperators.ValueNotLike(buffer);
      case STRING:
        return new StringFilterOperators.ValueNotLike(buffer);
      default:
        throw new UnsupportedOperationException(UNSUPPORTED_DATATYPE_MESSAGE + classSerializeId);
    }
  }

  public static Filter deserializeValueIsNullFilter(ByteBuffer buffer) {
    return new ValueIsNullOperator(buffer);
  }

  public static Filter deserializeValueIsNotNullFilter(ByteBuffer buffer) {
    return new ValueIsNotNullOperator(buffer);
  }
}
