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
package org.apache.iotdb.tsfile.write.chunk;

import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import java.io.IOException;
import java.util.List;

/**
 * A chunk group in TsFile contains several series. A ChunkGroupWriter should implement write method
 * which takes a timestamp(in TimeValue class) and a list of data points as input. It should also
 * provide flushing method for serializing to local file system or HDFS.
 */
public interface IChunkGroupWriter {

  /**
   * receive a timestamp and a list of data points, write them to their series writers.
   *
   * @param time - all data points have unify time stamp.
   * @param data - data point list to input
   * @throws WriteProcessException exception in write process
   * @throws IOException exception in IO
   */
  int write(long time, List<DataPoint> data) throws WriteProcessException, IOException;

  /**
   * receive a tablet, write it to chunk writers
   *
   * @throws WriteProcessException exception in write process
   * @throws IOException exception in IO
   */
  int write(Tablet tablet) throws WriteProcessException, IOException;

  /**
   * flushing method for serializing to local file system or HDFS. Implemented by
   * ChunkWriterImpl.writeToFileWriter().
   *
   * @param tsfileWriter - TSFileIOWriter
   * @throws IOException exception in IO
   * @return current ChunkGroupDataSize
   */
  long flushToFileWriter(TsFileIOWriter tsfileWriter) throws IOException;

  /**
   * get the max memory occupied at this time. Note that, this method should be called after running
   * {@code long calcAllocatedSize()}
   *
   * @return - allocated memory size.
   */
  long updateMaxGroupMemSize();

  /**
   * given a measurement descriptor, create a corresponding writer and put into this
   * ChunkGroupWriter.
   *
   * @param measurementSchema a measurement descriptor containing the message of the series
   */
  void tryToAddSeriesWriter(MeasurementSchema measurementSchema) throws IOException;

  /**
   * given a measurement descriptor list, create corresponding writers and put into this
   * ChunkGroupWriter.
   *
   * @param measurementSchemas
   */
  void tryToAddSeriesWriter(List<MeasurementSchema> measurementSchemas) throws IOException;

  /**
   * get the serialized size of current chunkGroup header + all chunks. Notice, the value does not
   * include any un-sealed page in the chunks.
   *
   * @return the serialized size of current chunkGroup header + all chunk
   */
  long getCurrentChunkGroupSize();
}
