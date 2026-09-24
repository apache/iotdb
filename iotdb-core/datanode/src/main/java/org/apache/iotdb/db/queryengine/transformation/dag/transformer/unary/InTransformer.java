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

package org.apache.iotdb.db.queryengine.transformation.dag.transformer.unary;

import org.apache.iotdb.calc.exception.QueryProcessException;
import org.apache.iotdb.db.queryengine.transformation.api.LayerReader;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Binary;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.apache.iotdb.db.utils.TypeServices.Transformation.IN_TRANSFORMER_COLUMN_TRANSFORMER_SERVICE;
import static org.apache.iotdb.db.utils.TypeServices.Transformation.IN_TRANSFORMER_SET_INITIALIZER_SERVICE;

public class InTransformer extends UnaryTransformer {

  private final Satisfy satisfy;

  private Set<Integer> intSet;
  private Set<Long> longSet;
  private Set<Float> floatSet;
  private Set<Double> doubleSet;
  private Set<Boolean> booleanSet;
  private Set<String> stringSet;

  public InTransformer(LayerReader layerReader, boolean isNotIn, Set<String> values) {
    super(layerReader);
    satisfy = isNotIn ? new NotInSatisfy() : new InSatisfy();
    initTypedSet(values);
  }

  private void initTypedSet(Set<String> values) {
    IN_TRANSFORMER_SET_INITIALIZER_SERVICE
        .call(Type.fromTsDataType(layerReaderDataType))
        .initialize(this, values);
  }

  @Override
  public TSDataType[] getDataTypes() {
    return new TSDataType[] {TSDataType.BOOLEAN};
  }

  @Override
  protected void transform(Column[] columns, ColumnBuilder builder)
      throws QueryProcessException, IOException {
    IN_TRANSFORMER_COLUMN_TRANSFORMER_SERVICE
        .call(Type.fromTsDataType(layerReaderDataType))
        .transform(this, columns, builder);
  }

  public void initIntSet(final Set<String> values) {
    intSet = new HashSet<>();
    for (final String value : values) {
      intSet.add(Integer.valueOf(value));
    }
  }

  public void initLongSet(final Set<String> values) {
    longSet = new HashSet<>();
    for (final String value : values) {
      longSet.add(Long.valueOf(value));
    }
  }

  public void initFloatSet(final Set<String> values) {
    floatSet = new HashSet<>();
    for (final String value : values) {
      floatSet.add(Float.valueOf(value));
    }
  }

  public void initDoubleSet(final Set<String> values) {
    doubleSet = new HashSet<>();
    for (final String value : values) {
      doubleSet.add(Double.valueOf(value));
    }
  }

  public void initBooleanSet(final Set<String> values) {
    booleanSet = new HashSet<>();
    for (final String value : values) {
      booleanSet.add(Boolean.valueOf(value));
    }
  }

  public void initStringSet(final Set<String> values) {
    stringSet = values;
  }

  public void transformInt(Column[] columns, ColumnBuilder builder) {
    int count = columns[0].getPositionCount();
    int[] values = columns[0].getInts();
    boolean[] isNulls = columns[0].isNull();

    for (int i = 0; i < count; i++) {
      if (!isNulls[i]) {
        boolean res = satisfy.of(values[i]);
        builder.writeBoolean(res);
      } else {
        builder.appendNull();
      }
    }
  }

  public void transformLong(Column[] columns, ColumnBuilder builder) {
    int count = columns[0].getPositionCount();
    long[] values = columns[0].getLongs();
    boolean[] isNulls = columns[0].isNull();

    for (int i = 0; i < count; i++) {
      if (!isNulls[i]) {
        boolean res = satisfy.of(values[i]);
        builder.writeBoolean(res);
      } else {
        builder.appendNull();
      }
    }
  }

  public void transformFloat(Column[] columns, ColumnBuilder builder) {
    int count = columns[0].getPositionCount();
    float[] values = columns[0].getFloats();
    boolean[] isNulls = columns[0].isNull();

    for (int i = 0; i < count; i++) {
      if (!isNulls[i]) {
        boolean res = satisfy.of(values[i]);
        builder.writeBoolean(res);
      } else {
        builder.appendNull();
      }
    }
  }

  public void transformDouble(Column[] columns, ColumnBuilder builder) {
    int count = columns[0].getPositionCount();
    double[] values = columns[0].getDoubles();
    boolean[] isNulls = columns[0].isNull();

    for (int i = 0; i < count; i++) {
      if (!isNulls[i]) {
        boolean res = satisfy.of(values[i]);
        builder.writeBoolean(res);
      } else {
        builder.appendNull();
      }
    }
  }

  public void transformBoolean(Column[] columns, ColumnBuilder builder) {
    int count = columns[0].getPositionCount();
    boolean[] values = columns[0].getBooleans();
    boolean[] isNulls = columns[0].isNull();

    for (int i = 0; i < count; i++) {
      if (!isNulls[i]) {
        boolean res = satisfy.of(values[i]);
        builder.writeBoolean(res);
      } else {
        builder.appendNull();
      }
    }
  }

  public void transformBinary(Column[] columns, ColumnBuilder builder) {
    int count = columns[0].getPositionCount();
    Binary[] values = columns[0].getBinaries();
    boolean[] isNulls = columns[0].isNull();

    for (int i = 0; i < count; i++) {
      if (!isNulls[i]) {
        boolean res = satisfy.of(values[i].getStringValue(TSFileConfig.STRING_CHARSET));
        builder.writeBoolean(res);
      } else {
        builder.appendNull();
      }
    }
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
