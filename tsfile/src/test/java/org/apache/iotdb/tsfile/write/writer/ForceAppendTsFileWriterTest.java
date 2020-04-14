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
package org.apache.iotdb.tsfile.write.writer;

import static org.junit.Assert.assertEquals;

import java.io.File;

import org.apache.iotdb.tsfile.constant.TestConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.FloatDataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.junit.Test;

public class ForceAppendTsFileWriterTest {
  private static final String FILE_NAME = TestConstant.BASE_OUTPUT_PATH.concat("test.tsfile");
  private static FSFactory fsFactory = FSFactoryProducer.getFSFactory();

  @Test
  public void test() throws Exception {
    File file = fsFactory.getFile(FILE_NAME);
    TsFileWriter writer = new TsFileWriter(file);
    writer.registerTimeseries(new Path("d1.s1"),
        new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE));
    writer.registerTimeseries(new Path("d1.s2"),
        new MeasurementSchema("s2", TSDataType.FLOAT, TSEncoding.RLE));
    writer.write(new TSRecord(1, "d1").addTuple(new FloatDataPoint("s1", 5))
        .addTuple(new FloatDataPoint("s2", 4)));
    writer.write(new TSRecord(2, "d1").addTuple(new FloatDataPoint("s1", 5))
        .addTuple(new FloatDataPoint("s2", 4)));
    writer.flushAllChunkGroups();

    long firstMetadataPosition = writer.getIOWriter().getPos();
    writer.close();
    ForceAppendTsFileWriter fwriter = new ForceAppendTsFileWriter(file);
    assertEquals(firstMetadataPosition, fwriter.getTruncatePosition());
  }

}
