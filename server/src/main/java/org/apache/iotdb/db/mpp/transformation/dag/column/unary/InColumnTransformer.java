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

package org.apache.iotdb.db.mpp.transformation.dag.column.unary;

import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.mpp.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.type.Type;
import org.apache.iotdb.tsfile.read.common.type.TypeEnum;

import java.util.HashSet;
import java.util.Set;

public class InColumnTransformer extends UnaryColumnTransformer {
  private final Satisfy satisfy;

  private final TypeEnum childType;

  private Set<Integer> intSet;
  private Set<Long> longSet;
  private Set<Float> floatSet;
  private Set<Double> doubleSet;
  private Set<Boolean> booleanSet;
  private Set<String> stringSet;

  public InColumnTransformer(
      Type returnType,
      ColumnTransformer childColumnTransformer,
      boolean isNotIn,
      Set<String> values) {
    super(returnType, childColumnTransformer);
    satisfy = isNotIn ? new NotInSatisfy() : new InSatisfy();
    this.childType =
        childColumnTransformer.getType() == null
            ? null
            : childColumnTransformer.getType().getTypeEnum();
    initTypedSet(values);
  }

  @Override
  protected void doTransform(Column column, ColumnBuilder columnBuilder) {
    for (int i = 0, n = column.getPositionCount(); i < n; i++) {
      if (!column.isNull(i)) {
        switch (childType) {
          case INT32:
            returnType.writeBoolean(columnBuilder, satisfy.of(column.getInt(i)));
            break;
          case INT64:
            returnType.writeBoolean(columnBuilder, satisfy.of(column.getLong(i)));
            break;
          case FLOAT:
            returnType.writeBoolean(columnBuilder, satisfy.of(column.getFloat(i)));
            break;
          case DOUBLE:
            returnType.writeBoolean(columnBuilder, satisfy.of(column.getDouble(i)));
            break;
          case BOOLEAN:
            returnType.writeBoolean(columnBuilder, satisfy.of(column.getBoolean(i)));
            break;
          case BINARY:
            returnType.writeBoolean(
                columnBuilder, satisfy.of(column.getBinary(i).getStringValue()));
            break;
          default:
            throw new UnsupportedOperationException("unsupported data type: " + childType);
        }
      } else {
        columnBuilder.appendNull();
      }
    }
  }

  private void initTypedSet(Set<String> values) {
    if (childType == null) {
      return;
    }
    switch (childType) {
      case INT32:
        intSet = new HashSet<>();
        for (String value : values) {
          try {
            intSet.add(Integer.valueOf(value));
          } catch (IllegalArgumentException e) {
            throw new SemanticException(
                String.format("\"%s\" cannot be cast to [%s]", value, childType));
          }
        }
        break;
      case INT64:
        longSet = new HashSet<>();
        for (String value : values) {
          try {
            longSet.add(Long.valueOf(value));
          } catch (IllegalArgumentException e) {
            throw new SemanticException(
                String.format("\"%s\" cannot be cast to [%s]", value, childType));
          }
        }
        break;
      case FLOAT:
        floatSet = new HashSet<>();
        for (String value : values) {
          try {
            floatSet.add(Float.valueOf(value));
          } catch (IllegalArgumentException e) {
            throw new SemanticException(
                String.format("\"%s\" cannot be cast to [%s]", value, childType));
          }
        }
        break;
      case DOUBLE:
        doubleSet = new HashSet<>();
        for (String value : values) {
          try {
            doubleSet.add(Double.valueOf(value));
          } catch (IllegalArgumentException e) {
            throw new SemanticException(
                String.format("\"%s\" cannot be cast to [%s]", value, childType));
          }
        }
        break;
      case BOOLEAN:
        booleanSet = new HashSet<>();
        for (String value : values) {
          booleanSet.add(strictCastToBool(value));
        }
        break;
      case BINARY:
        stringSet = values;
        break;
      default:
        throw new UnsupportedOperationException("unsupported data type: " + childType);
    }
  }

  private boolean strictCastToBool(String s) {
    if ("true".equalsIgnoreCase(s)) {
      return true;
    } else if ("false".equalsIgnoreCase(s)) {
      return false;
    }
    throw new SemanticException(String.format("\"%s\" cannot be cast to [BOOLEAN]", s));
  }

  private interface Satisfy {

    boolean of(int intValue);

    boolean of(long longValue);

    boolean of(float floatValue);

    boolean of(double doubleValue);

    boolean of(boolean booleanValue);

    boolean of(String stringValue);
  }

  private class InSatisfy implements Satisfy {

    @Override
    public boolean of(int intValue) {
      return intSet.contains(intValue);
    }

    @Override
    public boolean of(long longValue) {
      return longSet.contains(longValue);
    }

    @Override
    public boolean of(float floatValue) {
      return floatSet.contains(floatValue);
    }

    @Override
    public boolean of(double doubleValue) {
      return doubleSet.contains(doubleValue);
    }

    @Override
    public boolean of(boolean booleanValue) {
      return booleanSet.contains(booleanValue);
    }

    @Override
    public boolean of(String stringValue) {
      return stringSet.contains(stringValue);
    }
  }

  private class NotInSatisfy implements Satisfy {

    @Override
    public boolean of(int intValue) {
      return !intSet.contains(intValue);
    }

    @Override
    public boolean of(long longValue) {
      return !longSet.contains(longValue);
    }

    @Override
    public boolean of(float floatValue) {
      return !floatSet.contains(floatValue);
    }

    @Override
    public boolean of(double doubleValue) {
      return !doubleSet.contains(doubleValue);
    }

    @Override
    public boolean of(boolean booleanValue) {
      return !booleanSet.contains(booleanValue);
    }

    @Override
    public boolean of(String stringValue) {
      return !stringSet.contains(stringValue);
    }
  }
}
