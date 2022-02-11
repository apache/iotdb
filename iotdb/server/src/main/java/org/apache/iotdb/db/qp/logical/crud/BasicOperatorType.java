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
package org.apache.iotdb.db.qp.logical.crud;

import org.apache.iotdb.db.exception.query.LogicalOperatorException;
import org.apache.iotdb.db.exception.runtime.SQLParserException;
import org.apache.iotdb.db.qp.constant.FilterConstant;
import org.apache.iotdb.db.qp.constant.FilterConstant.FilterType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IUnaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.ValueFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import static org.apache.iotdb.db.conf.IoTDBConstant.TIME;

/** all basic operator in filter. */
public enum BasicOperatorType {
  EQ {
    @Override
    public <T extends Comparable<T>> IUnaryExpression getUnaryExpression(Path path, T value) {
      if (path.equals(TIME)) {
        return new GlobalTimeExpression(TimeFilter.eq((Long) value));
      } else {
        return new SingleSeriesExpression(path, ValueFilter.eq(value));
      }
    }

    @Override
    public <T extends Comparable<T>> Filter getValueFilter(T value) {
      return ValueFilter.eq(value);
    }

    @Override
    public Filter getTimeFilter(long value) {
      return TimeFilter.eq(value);
    }
  },
  LTEQ {
    @Override
    public <T extends Comparable<T>> IUnaryExpression getUnaryExpression(Path path, T value) {
      if (path.equals(TIME)) {
        return new GlobalTimeExpression(TimeFilter.ltEq((Long) value));
      } else {
        return new SingleSeriesExpression(path, ValueFilter.ltEq(value));
      }
    }

    @Override
    public <T extends Comparable<T>> Filter getValueFilter(T value) {
      return ValueFilter.ltEq(value);
    }

    @Override
    public Filter getTimeFilter(long value) {
      return TimeFilter.ltEq(value);
    }
  },
  LT {
    @Override
    public <T extends Comparable<T>> IUnaryExpression getUnaryExpression(Path path, T value) {
      if (path.equals(TIME)) {
        return new GlobalTimeExpression(TimeFilter.lt((Long) value));
      } else {
        return new SingleSeriesExpression(path, ValueFilter.lt(value));
      }
    }

    @Override
    public <T extends Comparable<T>> Filter getValueFilter(T value) {
      return ValueFilter.lt(value);
    }

    @Override
    public Filter getTimeFilter(long value) {
      return TimeFilter.lt(value);
    }
  },
  GTEQ {
    @Override
    public <T extends Comparable<T>> IUnaryExpression getUnaryExpression(Path path, T value) {
      if (path.equals(TIME)) {
        return new GlobalTimeExpression(TimeFilter.gtEq((Long) value));
      } else {
        return new SingleSeriesExpression(path, ValueFilter.gtEq(value));
      }
    }

    @Override
    public <T extends Comparable<T>> Filter getValueFilter(T value) {
      return ValueFilter.gtEq(value);
    }

    @Override
    public Filter getTimeFilter(long value) {
      return TimeFilter.gtEq(value);
    }
  },
  GT {
    @Override
    public <T extends Comparable<T>> IUnaryExpression getUnaryExpression(Path path, T value) {
      if (path.equals(TIME)) {
        return new GlobalTimeExpression(TimeFilter.gt((Long) value));
      } else {
        return new SingleSeriesExpression(path, ValueFilter.gt(value));
      }
    }

    @Override
    public <T extends Comparable<T>> Filter getValueFilter(T value) {
      return ValueFilter.gt(value);
    }

    @Override
    public Filter getTimeFilter(long value) {
      return TimeFilter.gt(value);
    }
  },
  NOTEQUAL {
    @Override
    public <T extends Comparable<T>> IUnaryExpression getUnaryExpression(Path path, T value) {
      if (path.equals(TIME)) {
        return new GlobalTimeExpression(TimeFilter.notEq((Long) value));
      } else {
        return new SingleSeriesExpression(path, ValueFilter.notEq(value));
      }
    }

    @Override
    public <T extends Comparable<T>> Filter getValueFilter(T value) {
      return ValueFilter.notEq(value);
    }

    @Override
    public Filter getTimeFilter(long value) {
      return TimeFilter.notEq(value);
    }
  };

  /**
   * BasicOperatorType Constructor.
   *
   * @param filterType token in Int Type
   * @return basic operator type
   * @throws LogicalOperatorException Logical Operator Exception
   */
  public static BasicOperatorType getBasicOpBySymbol(FilterType filterType)
      throws SQLParserException {
    switch (filterType) {
      case EQUAL:
        return EQ;
      case LESSTHANOREQUALTO:
        return LTEQ;
      case LESSTHAN:
        return LT;
      case GREATERTHANOREQUALTO:
        return GTEQ;
      case GREATERTHAN:
        return GT;
      case NOTEQUAL:
        return NOTEQUAL;
      default:
        throw new SQLParserException(
            "unsupported type:{}" + FilterConstant.filterNames.get(filterType));
    }
  }

  public abstract <T extends Comparable<T>> IUnaryExpression getUnaryExpression(Path path, T value);

  public abstract <T extends Comparable<T>> Filter getValueFilter(T tsPrimitiveType);

  public abstract Filter getTimeFilter(long value);
}
