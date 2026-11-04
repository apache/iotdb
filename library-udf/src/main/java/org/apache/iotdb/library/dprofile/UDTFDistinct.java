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

import org.apache.iotdb.library.util.TypeServices;
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
  private DistinctOperations operations;

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
    Type dataType = parameters.getDataType(0);
    operations =
        TypeServices.scalarService(
                BOOLEAN_OPERATIONS,
                INT_OPERATIONS,
                LONG_OPERATIONS,
                FLOAT_OPERATIONS,
                DOUBLE_OPERATIONS,
                TEXT_OPERATIONS,
                UNSUPPORTED_OPERATIONS)
            .call(TypeServices.toReadType(dataType));
    operations.initialize(this);
  }

  @Override
  public void transform(Row row, PointCollector pc) throws Exception {
    operations.add(this, row);
  }

  @Override
  public void terminate(PointCollector pc) throws Exception {
    operations.terminate(this, pc);
  }

  @Override
  public void beforeDestroy() {
    operations.clear(this);
  }

  private static final DistinctOperations INT_OPERATIONS =
      new DistinctOperations() {
        public void initialize(UDTFDistinct target) {
          target.intSet = new IntHashSet();
        }

        public void add(UDTFDistinct target, Row row) throws Exception {
          target.intSet.add(row.getInt(0));
        }

        public void terminate(UDTFDistinct target, PointCollector collector) throws Exception {
          MutableIntIterator iterator = target.intSet.intIterator();
          int i = 0;
          while (iterator.hasNext()) {
            collector.putInt(i++, iterator.next());
          }
        }

        public void clear(UDTFDistinct target) {
          target.intSet.clear();
        }
      };
  private static final DistinctOperations LONG_OPERATIONS =
      new DistinctOperations() {
        public void initialize(UDTFDistinct target) {
          target.longSet = new LongHashSet();
        }

        public void add(UDTFDistinct target, Row row) throws Exception {
          target.longSet.add(row.getLong(0));
        }

        public void terminate(UDTFDistinct target, PointCollector collector) throws Exception {
          MutableLongIterator iterator = target.longSet.longIterator();
          int i = 0;
          while (iterator.hasNext()) {
            collector.putLong(i++, iterator.next());
          }
        }

        public void clear(UDTFDistinct target) {
          target.longSet.clear();
        }
      };
  private static final DistinctOperations FLOAT_OPERATIONS =
      new DistinctOperations() {
        public void initialize(UDTFDistinct target) {
          target.floatSet = new FloatHashSet();
        }

        public void add(UDTFDistinct target, Row row) throws Exception {
          target.floatSet.add(row.getFloat(0));
        }

        public void terminate(UDTFDistinct target, PointCollector collector) throws Exception {
          MutableFloatIterator iterator = target.floatSet.floatIterator();
          int i = 0;
          while (iterator.hasNext()) {
            collector.putFloat(i++, iterator.next());
          }
        }

        public void clear(UDTFDistinct target) {
          target.floatSet.clear();
        }
      };
  private static final DistinctOperations DOUBLE_OPERATIONS =
      new DistinctOperations() {
        public void initialize(UDTFDistinct target) {
          target.doubleSet = new DoubleHashSet();
        }

        public void add(UDTFDistinct target, Row row) throws Exception {
          target.doubleSet.add(row.getDouble(0));
        }

        public void terminate(UDTFDistinct target, PointCollector collector) throws Exception {
          MutableDoubleIterator iterator = target.doubleSet.doubleIterator();
          int i = 0;
          while (iterator.hasNext()) {
            collector.putDouble(i++, iterator.next());
          }
        }

        public void clear(UDTFDistinct target) {
          target.doubleSet.clear();
        }
      };
  private static final DistinctOperations BOOLEAN_OPERATIONS =
      new DistinctOperations() {
        public void initialize(UDTFDistinct target) {
          target.booleanSet = new BooleanHashSet();
        }

        public void add(UDTFDistinct target, Row row) throws Exception {
          target.booleanSet.add(row.getBoolean(0));
        }

        public void terminate(UDTFDistinct target, PointCollector collector) throws Exception {
          MutableBooleanIterator iterator = target.booleanSet.booleanIterator();
          int i = 0;
          while (iterator.hasNext()) {
            collector.putBoolean(i++, iterator.next());
          }
        }

        public void clear(UDTFDistinct target) {
          target.booleanSet.clear();
        }
      };
  private static final DistinctOperations TEXT_OPERATIONS =
      new DistinctOperations() {
        public void initialize(UDTFDistinct target) {
          target.stringSet = new HashSet<>();
        }

        public void add(UDTFDistinct target, Row row) throws Exception {
          target.stringSet.add(row.getString(0));
        }

        public void terminate(UDTFDistinct target, PointCollector collector) throws Exception {
          int i = 0;
          for (String value : target.stringSet) {
            collector.putString(i++, value);
          }
        }

        public void clear(UDTFDistinct target) {
          target.stringSet.clear();
        }
      };
  private static final DistinctOperations UNSUPPORTED_OPERATIONS =
      new DistinctOperations() {
        public void initialize(UDTFDistinct target) {}

        public void add(UDTFDistinct target, Row row) {}

        public void terminate(UDTFDistinct target, PointCollector collector) {}

        public void clear(UDTFDistinct target) {}
      };

  private interface DistinctOperations {
    void initialize(UDTFDistinct target) throws Exception;

    void add(UDTFDistinct target, Row row) throws Exception;

    void terminate(UDTFDistinct target, PointCollector collector) throws Exception;

    void clear(UDTFDistinct target);
  }
}
