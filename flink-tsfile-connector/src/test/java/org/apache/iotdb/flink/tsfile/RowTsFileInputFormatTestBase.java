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

package org.apache.iotdb.flink.tsfile;

import org.apache.iotdb.flink.util.TsFileWriteUtil;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;

import org.apache.flink.types.Row;
import org.junit.Before;

import java.io.File;

/** Base class for TsFileInputFormat tests. */
public abstract class RowTsFileInputFormatTestBase extends RowTsFileConnectorTestBase {

  protected String sourceTsFilePath1;
  protected String sourceTsFilePath2;
  protected RowRowRecordParser parser = RowRowRecordParser.create(rowTypeInfo, paths);
  protected QueryExpression queryExpression = QueryExpression.create(paths, null);

  @Before
  public void prepareSourceTsFile() throws Exception {
    sourceTsFilePath1 = String.join(File.separator, tmpDir, "source1.tsfile");
    sourceTsFilePath2 = String.join(File.separator, tmpDir, "source2.tsfile");
    TsFileWriteUtil.create1(sourceTsFilePath1);
    TsFileWriteUtil.create2(sourceTsFilePath2);
  }

  protected TsFileInputFormat<Row> prepareInputFormat(String filePath) {
    return new TsFileInputFormat<>(filePath, queryExpression, parser, config);
  }
}
