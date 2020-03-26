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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;

import org.apache.flink.types.Row;
import org.apache.iotdb.flink.tool.TsFileWriteTool;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.constant.QueryConstant;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Base class of the TsFileInputFormat tests.
 */
public abstract class RowTsFileInputFormatTestBase {

	protected String tmpDir;
	protected String sourceTsFilePath1;
	protected String sourceTsFilePath2;
	protected RowTypeInfo rowTypeInfo;
	protected TSFileConfig config;
	protected RowRowRecordParser parser;
	protected QueryExpression queryExpression;

	@Before
	public void prepareSourceTsFile() throws Exception {
		tmpDir = String.join(
			File.separator,
			TsFileWriteTool.TMP_DIR,
			UUID.randomUUID().toString());
		new File(tmpDir).mkdirs();
		sourceTsFilePath1 = String.join(
			File.separator,
			tmpDir, "source1.tsfile");
		sourceTsFilePath2 = String.join(
			File.separator,
			tmpDir, "source2.tsfile");
		TsFileWriteTool.create1(sourceTsFilePath1);
		TsFileWriteTool.create2(sourceTsFilePath2);
	}

	@After
	public void removeSourceTsFile() {
		File sourceTsFile1 = new File(sourceTsFilePath1);
		if (sourceTsFile1.exists()) {
			sourceTsFile1.delete();
		}
		File sourceTsFile2 = new File(sourceTsFilePath2);
		if (sourceTsFile2.exists()) {
			sourceTsFile2.delete();
		}
		File tmpDirFile = new File(tmpDir);
		if (tmpDirFile.exists()) {
			tmpDirFile.delete();
		}
	}

	protected TsFileInputFormat<Row> prepareInputFormat(String filePath) {
		String[] filedNames = {
			QueryConstant.RESERVED_TIME,
			"device_1.sensor_1",
			"device_1.sensor_2",
			"device_1.sensor_3",
			"device_2.sensor_1",
			"device_2.sensor_2",
			"device_2.sensor_3"
		};
		TypeInformation[] typeInformations = new TypeInformation[] {
			Types.LONG,
			Types.FLOAT,
			Types.INT,
			Types.INT,
			Types.FLOAT,
			Types.INT,
			Types.INT
		};
		List<Path> paths = Arrays.stream(filedNames)
			.filter(s -> !s.equals(QueryConstant.RESERVED_TIME))
			.map(Path::new)
			.collect(Collectors.toList());
		config = new TSFileConfig();
		config.setBatchSize(500);
		rowTypeInfo = new RowTypeInfo(typeInformations, filedNames);
		queryExpression = QueryExpression.create(paths, null);
		parser = RowRowRecordParser.create(rowTypeInfo, queryExpression.getSelectedSeries());
		return new TsFileInputFormat<>(filePath, queryExpression, parser, config);
	}
}
