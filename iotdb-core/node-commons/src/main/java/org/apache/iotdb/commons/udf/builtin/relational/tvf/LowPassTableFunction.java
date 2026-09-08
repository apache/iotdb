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

package org.apache.iotdb.commons.udf.builtin.relational.tvf;

import org.apache.iotdb.commons.udf.builtin.relational.tvf.fft.DoubleFFT_1D;
import org.apache.iotdb.udf.api.relational.table.MapTableFunctionHandle;
import org.apache.iotdb.udf.api.relational.table.TableFunctionHandle;
import org.apache.iotdb.udf.api.relational.table.TableFunctionProcessorProvider;
import org.apache.iotdb.udf.api.relational.table.processor.TableFunctionDataProcessor;
import org.apache.iotdb.udf.api.type.Type;

public class LowPassTableFunction extends FilterTransferTableFunction {

  @Override
  public TableFunctionProcessorProvider getProcessorProvider(
      TableFunctionHandle tableFunctionHandle) {
    MapTableFunctionHandle handle = (MapTableFunctionHandle) tableFunctionHandle;
    double wpass = (double) handle.getProperty(WPASS);
    Type[] partitionTypes =
        WindowTVFUtils.parseTypes((String) handle.getProperty(PARTITION_TYPES_PROPERTY));
    int calculationColumnCount = (Integer) handle.getProperty(CALCULATION_COLUMN_COUNT_PROPERTY);

    return new TableFunctionProcessorProvider() {
      @Override
      public TableFunctionDataProcessor getDataProcessor() {
        return new LowPassDataProcessor(wpass, partitionTypes, calculationColumnCount);
      }
    };
  }

  @Override
  protected String convertColumnName(String columnName) {
    return String.format("lowpass(%s)", columnName);
  }

  protected static class LowPassDataProcessor extends FilterTransferDataProcessor {

    public LowPassDataProcessor(double wpass, Type[] partitionTypes, int calculationColumnCount) {
      super(wpass, partitionTypes, calculationColumnCount);
    }

    @Override
    protected double[] filterTransform(
        CalculationColumnContainer columnContainer, int size, double wpass) {
      DoubleFFT_1D fft = new DoubleFFT_1D(size);
      double[] temp = new double[2 * size];
      for (int i = 0; i < size; i++) {
        temp[2 * i] = columnContainer.validValues[i];
        temp[2 * i + 1] = 0;
      }
      fft.complexForward(temp);
      int m = (int) Math.ceil(wpass * size / 2);
      for (int i = 2 * m; i <= 2 * (size - m) + 1; i++) {
        temp[i] = 0;
      }
      fft.complexInverse(temp, true);
      return temp;
    }
  }
}
