/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.tsfile.write.chunk;

import org.apache.iotdb.tsfile.file.footer.ChunkGroupFooter;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.footer.ChunkGroupFooter;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import java.io.IOException;
import java.util.List;

/**
 * a chunk group in TSFile contains a list of value series. TimeSeriesGroupWriter should implement write method which
 * inputs a time stamp(in TimeValue class) and a list of data points. It also should provide flushing method for
 * outputting to OS file system or HDFS.
 *
 * @author kangrong
 */
public interface IChunkGroupWriter {
    /**
     * receive a timestamp and a list of data points, write them to their series writers.
     *
     * @param time
     *            - all data points have unify time stamp.
     * @param data
     *            - data point list to input
     * @throws WriteProcessException
     *             exception in write process
     * @throws IOException
     *             exception in IO
     */
    void write(long time, List<DataPoint> data) throws WriteProcessException, IOException;

    /**
     * flushing method for outputting to OS file system or HDFS. Implemented by ChunkWriterImpl.writeToFileWriter()
     *
     * @param tsfileWriter
     *            - TSFileIOWriter
     * @throws IOException
     *             exception in IO
     */
    ChunkGroupFooter flushToFileWriter(TsFileIOWriter tsfileWriter) throws IOException;

    /**
     * get the max memory occupied at this time.
     *
     * Note that, this method should be called after running {@code long calcAllocatedSize()}
     *
     * @return - allocated memory size.
     */
    long updateMaxGroupMemSize();

    /**
     * given a measurement descriptor, create a corresponding writer and put into this ChunkGroupWriter
     *
     * @param measurementSchema
     *            a measurement descriptor containing the message of the series
     * @param pageSize
     *            the specified page size
     */
    void addSeriesWriter(MeasurementSchema measurementSchema, int pageSize);

    /**
     * @return get the serialized size of current chunkGroup header + all chunks. Notice, the value does not include any
     *         un-sealed page in the chunks.
     */
    long getCurrentChunkGroupSize();

    int getSeriesNumber();
}
