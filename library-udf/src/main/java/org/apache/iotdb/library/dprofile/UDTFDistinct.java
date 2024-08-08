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

package org.apache.iotdb.library.dprofile;

import org.apache.iotdb.commons.udf.utils.UDFDataTypeTransformer;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

import org.eclipse.collections.api.iterator.MutableBooleanIterator;
import org.eclipse.collections.api.iterator.MutableDoubleIterator;
import org.eclipse.collections.api.iterator.MutableFloatIterator;
import org.eclipse.collections.api.iterator.MutableIntIterator;
import org.eclipse.collections.api.iterator.MutableLongIterator;
import org.eclipse.collections.impl.set.mutable.primitive.BooleanHashSet;
import org.eclipse.collections.impl.set.mutable.primitive.DoubleHashSet;
import org.eclipse.collections.impl.set.mutable.primitive.FloatHashSet;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;

import java.util.HashSet;

/** This function counts number of distinct values of input series. */
public class UDTFDistinct implements UDTF {

  private IntHashSet intSet;
  private LongHashSet longSet;
  private FloatHashSet floatSet;
  private DoubleHashSet doubleSet;
  private BooleanHashSet booleanSet;
  private HashSet<String> stringSet;
  private TSDataType dataType;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(
            0, Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE, Type.TEXT, Type.BOOLEAN);
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations
        .setAccessStrategy(new RowByRowAccessStrategy())
        .setOutputDataType(parameters.getDataType(0));
    dataType = UDFDataTypeTransformer.transformToTsDataType(parameters.getDataType(0));
    switch (dataType) {
      case INT32:
        intSet = new IntHashSet();
        break;
      case INT64:
        longSet = new LongHashSet();
        break;
      case FLOAT:
        floatSet = new FloatHashSet();
        break;
      case DOUBLE:
        doubleSet = new DoubleHashSet();
        break;
      case TEXT:
        stringSet = new HashSet<>();
        break;
      case BOOLEAN:
        booleanSet = new BooleanHashSet();
    }
  }

  @Override
  public void transform(Row row, PointCollector pc) throws Exception {
    switch (dataType) {
      case INT32:
        intSet.add(row.getInt(0));
        break;
      case INT64:
        longSet.add(row.getLong(0));
        break;
      case FLOAT:
        floatSet.add(row.getFloat(0));
        break;
      case DOUBLE:
        doubleSet.add(row.getDouble(0));
        break;
      case TEXT:
        stringSet.add(row.getString(0));
        break;
      case BOOLEAN:
        booleanSet.add(row.getBoolean(0));
    }
  }

  @Override
  public void terminate(PointCollector pc) throws Exception {
    int i = 0;
    switch (dataType) {
      case INT32:
        MutableIntIterator intIterator = intSet.intIterator();
        while (intIterator.hasNext()) {
          pc.putInt(i, intIterator.next());
          i++;
        }
        break;
      case INT64:
        MutableLongIterator longIterator = longSet.longIterator();
        while (longIterator.hasNext()) {
          pc.putLong(i, longIterator.next());
          i++;
        }
        break;
      case FLOAT:
        MutableFloatIterator floatIterator = floatSet.floatIterator();
        while (floatIterator.hasNext()) {
          pc.putFloat(i, floatIterator.next());
          i++;
        }
        break;
      case DOUBLE:
        MutableDoubleIterator doubleIterator = doubleSet.doubleIterator();
        while (doubleIterator.hasNext()) {
          pc.putDouble(i, doubleIterator.next());
          i++;
        }
        break;
      case TEXT:
        for (String s : stringSet) {
          pc.putString(i, s);
          i++;
        }
        break;
      case BOOLEAN:
        MutableBooleanIterator booleanIterator = booleanSet.booleanIterator();
        while (booleanIterator.hasNext()) {
          pc.putBoolean(i, booleanIterator.next());
          i++;
        }
    }
  }

  @Override
  public void beforeDestroy() {
    switch (dataType) {
      case INT32:
        intSet.clear();
        break;
      case INT64:
        longSet.clear();
        break;
      case FLOAT:
        floatSet.clear();
        break;
      case DOUBLE:
        doubleSet.clear();
        break;
      case TEXT:
        stringSet.clear();
        break;
      case BOOLEAN:
        booleanSet.clear();
    }
  }
}
