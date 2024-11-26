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

package org.apache.tsfile.read;

import org.apache.tsfile.utils.FileGenerator;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class TsFileSequenceReaderTimeseriesMetadataIteratorTest {

  private static final String FILE_PATH = FileGenerator.outputDataFile;
  private TsFileReader tsFile;

  @Before
  public void before() throws IOException {
    // create 4040 timeseries, 101 measurements per device.
    FileGenerator.generateFile(100, 40, 101);
    TsFileSequenceReader fileReader = new TsFileSequenceReader(FILE_PATH);
    tsFile = new TsFileReader(fileReader);
  }

  @After
  public void after() throws IOException {
    tsFile.close();
    FileGenerator.after();
  }

  @Test
  public void testReadTsFileTimeseriesMetadata() throws IOException {
    TsFileSequenceReader fileReader = new TsFileSequenceReader(FILE_PATH);
    TsFileSequenceReaderTimeseriesMetadataIterator iterator =
        new TsFileSequenceReaderTimeseriesMetadataIterator(fileReader, false);

    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals(4000, iterator.next().values().stream().mapToLong(List::size).sum());
    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals(40, iterator.next().values().stream().mapToLong(List::size).sum());
  }
}
