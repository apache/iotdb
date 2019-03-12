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

package org.apache.iotdb.db.query.aggregation;

import java.io.IOException;
import java.util.List;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.query.reader.merge.EngineReaderByTimeStamp;
import org.apache.iotdb.db.query.timegenerator.EngineTimeGenerator;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.utils.Binary;

public abstract class AggregateFunction {

  public String name;
  public BatchData resultData;
  public TSDataType dataType;
  public boolean hasSetValue;

  /**
   * construct.
   *
   * @param name aggregate function name.
   * @param dataType series data type.
   */
  public AggregateFunction(String name, TSDataType dataType) {
    this.name = name;
    this.dataType = dataType;
    this.hasSetValue = false;
    resultData = new BatchData(dataType, true, true);
  }

  public abstract void init();

  public abstract BatchData getResult();

  /**
   * <p>
   * Calculate the aggregation using <code>PageHeader</code>.
   * </p>
   *
   * @param pageHeader <code>PageHeader</code>
   */
  public abstract void calculateValueFromPageHeader(PageHeader pageHeader)
      throws ProcessorException;

  /**
   * <p>
   * Could not calculate using <method>calculateValueFromPageHeader</method> directly. Calculate the
   * aggregation according to all decompressed data in this page.
   * </p>
   *
   * @param dataInThisPage the data in the DataPage
   * @param unsequenceReader unsequence data reader
   * @throws IOException TsFile data read exception
   * @throws ProcessorException wrong aggregation method parameter
   */
  public abstract void calculateValueFromPageData(BatchData dataInThisPage,
      IPointReader unsequenceReader) throws IOException, ProcessorException;

  public abstract void calculateValueFromUnsequenceReader(IPointReader unsequenceReader)
      throws IOException, ProcessorException;


  /**
   * <p>
   * This method is calculate the aggregation using the common timestamps of cross series filter.
   * </p>
   *
   * @throws IOException TsFile data read error
   * @throws ProcessorException wrong aggregation method parameter
   */
  public abstract void calcAggregationUsingTimestamps(List<Long> timestamps,
      EngineReaderByTimeStamp dataReader) throws IOException, ProcessorException;

  /**
   * <p>
   * This method is calculate the group by function.
   * </p>
   */
  public abstract void calcGroupByAggregation(long partitionStart, long partitionEnd,
      long intervalStart, long intervalEnd,
      BatchData data) throws ProcessorException;

  /**
   * Convert a value from string to its real data type and put into return data.
   */
  public void putValueFromStr(String valueStr) throws ProcessorException {
    try {
      switch (dataType) {
        case INT32:
          resultData.putInt(Integer.parseInt(valueStr));
          break;
        case INT64:
          resultData.putLong(Long.parseLong(valueStr));
          break;
        case BOOLEAN:
          resultData.putBoolean(Boolean.parseBoolean(valueStr));
          break;
        case TEXT:
          resultData.putBinary(new Binary(valueStr));
          break;
        case DOUBLE:
          resultData.putDouble(Double.parseDouble(valueStr));
          break;
        case FLOAT:
          resultData.putFloat(Float.parseFloat(valueStr));
          break;
        default:
          throw new ProcessorException("Unsupported type " + dataType);
      }
    } catch (Exception e) {
      throw new ProcessorException(e.getMessage());
    }
  }
}
