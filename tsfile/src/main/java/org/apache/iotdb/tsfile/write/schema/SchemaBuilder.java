/**
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
package org.apache.iotdb.tsfile.write.schema;

import java.util.Map;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

/**
 * This class is used to build Schema of tsfile.
 */
public class SchemaBuilder {

  /**
   * the Schema which is being built.
   **/
  private Schema schema;

  /**
   * init schema by default value.
   */
  public SchemaBuilder() {
    schema = new Schema();
  }

  /**
   * add one series to TsFile schema.
   *
   * @param measurementId (not null) id of the series
   * @param dataType (not null) series data type
   * @param tsEncoding (not null) encoding method you specified
   * @param props information in encoding method. For RLE, Encoder.MAX_POINT_NUMBER For PLAIN,
   * Encoder.maxStringLength
   * @return this
   */
  public SchemaBuilder addSeries(String measurementId, TSDataType dataType, TSEncoding tsEncoding,
      CompressionType type, Map<String, String> props) {
    MeasurementSchema md = new MeasurementSchema(measurementId, dataType, tsEncoding, type, props);
    schema.registerMeasurement(md);
    return this;
  }

  /**
   * add one series to tsfile schema.
   *
   * @param measurementId (not null) id of the series
   * @param dataType (not null) series data type
   * @param tsEncoding (not null) encoding method you specified
   * @return this
   */
  public SchemaBuilder addSeries(String measurementId, TSDataType dataType, TSEncoding tsEncoding) {
    MeasurementSchema md = new MeasurementSchema(measurementId, dataType, tsEncoding);
    schema.registerMeasurement(md);
    return this;
  }

  /**
   * MeasurementSchema is the schema of one series.
   *
   * @param descriptor series schema
   * @return schema builder
   */
  public SchemaBuilder addSeries(MeasurementSchema descriptor) {
    schema.registerMeasurement(descriptor);
    return this;
  }

  /**
   * get file schema after adding all series and properties.
   *
   * @return constructed file schema
   */
  public Schema build() {
    return this.schema;
  }
}
