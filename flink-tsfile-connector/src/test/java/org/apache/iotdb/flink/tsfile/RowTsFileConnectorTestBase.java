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
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.constant.QueryConstant;
import org.apache.iotdb.tsfile.read.common.Path;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.util.FileUtils;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/** Base class of the TsFile connector tests. */
public abstract class RowTsFileConnectorTestBase {

  protected String tmpDir;
  protected TSFileConfig config = new TSFileConfig();
  protected String[] filedNames = {
    QueryConstant.RESERVED_TIME,
    "device_1.sensor_1",
    "device_1.sensor_2",
    "device_1.sensor_3",
    "device_2.sensor_1",
    "device_2.sensor_2",
    "device_2.sensor_3"
  };
  protected TypeInformation[] typeInformations =
      new TypeInformation[] {
        Types.LONG, Types.FLOAT, Types.INT, Types.INT, Types.FLOAT, Types.INT, Types.INT
      };
  protected List<Path> paths =
      Arrays.stream(filedNames)
          .filter(s -> !s.equals(QueryConstant.RESERVED_TIME))
          .map(s -> new Path(s, true))
          .collect(Collectors.toList());
  protected RowTypeInfo rowTypeInfo = new RowTypeInfo(typeInformations, filedNames);

  @Before
  public void prepareTempDirectory() {
    tmpDir = String.join(File.separator, TsFileWriteUtil.TMP_DIR, UUID.randomUUID().toString());
    new File(tmpDir).mkdirs();
    config.setBatchSize(500);
  }

  @After
  public void cleanTempDirectory() {
    File tmpDirFile = new File(tmpDir);
    FileUtils.deleteDirectoryQuietly(tmpDirFile);
  }
}
