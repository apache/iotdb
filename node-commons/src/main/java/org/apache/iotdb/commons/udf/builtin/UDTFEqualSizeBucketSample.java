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

package org.apache.iotdb.commons.udf.builtin;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.udf.utils.UDFDataTypeTransformer;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.type.Type;

public abstract class UDTFEqualSizeBucketSample implements UDTF {

  protected TSDataType dataType;
  protected double proportion;
  protected int bucketSize;

  @Override
  public void validate(UDFParameterValidator validator) throws MetadataException, UDFException {
    proportion = validator.getParameters().getDoubleOrDefault("proportion", 0.1);
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(0, Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE)
        .validate(
            proportion -> (double) proportion > 0 && (double) proportion <= 1,
            "Illegal sample proportion. proportion > 0 and proportion <= 1",
            proportion);
    dataType =
        UDFDataTypeTransformer.transformToTsDataType(validator.getParameters().getDataType(0));
    bucketSize = (int) (1 / proportion);
  }
}
