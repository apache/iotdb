/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.engine.compaction.utils;

import java.io.File;
import java.io.IOException;

public class CompactionFileInfo {
	String logicalStorageGroup;
	String virtualStorageGroupId;
	long timePartition;
	String filename;
	boolean sequence;

	private CompactionFileInfo(
					String logicalStorageGroup,
					String virtualStorageGroupId,
					long timePartition,
					String filename,
					boolean sequence) {
		this.logicalStorageGroup = logicalStorageGroup;
		this.virtualStorageGroupId = virtualStorageGroupId;
		this.timePartition = timePartition;
		this.filename = filename;
		this.sequence = sequence;
	}

	public static CompactionFileInfo parseCompactionFileInfo(String infoString) throws IOException {
		String[] info = infoString.split(" ");
		try {
			return new CompactionFileInfo(
							info[0], info[1], Long.parseLong(info[2]), info[3], info[4].equals("sequence"));
		} catch (Exception e) {
			throw new IOException("invalid compaction log line: " + infoString);
		}
	}

	public static CompactionFileInfo parseCompactionFileInfoFromPath(String filePath)
					throws IOException {
		String separator = File.separator;
		if (separator.equals("\\")) {
			separator = "\\\\";
		}
		String[] splitFilePath = filePath.split(separator);
		int pathLength = splitFilePath.length;
		if (pathLength < 4) {
			throw new IOException("invalid compaction file path: " + filePath);
		}
		try {
			return new CompactionFileInfo(
							splitFilePath[pathLength - 4],
							splitFilePath[pathLength - 3],
							Long.parseLong(splitFilePath[pathLength - 2]),
							splitFilePath[pathLength - 1],
							splitFilePath[pathLength - 5].equals("sequence"));
		} catch (Exception e) {
			throw new IOException("invalid compaction log line: " + filePath);
		}
	}

	public File getFile(String dataDir) {
		return new File(
						dataDir
										+ File.separator
										+ (sequence ? "sequence" : "unsequence")
										+ File.separator
										+ logicalStorageGroup
										+ File.separator
										+ virtualStorageGroupId
										+ File.separator
										+ timePartition
										+ File.separator
										+ filename);
	}

	public String getFilename() {
		return filename;
	}

	@Override
	public String toString() {
		return String.format("%s %s %d %s %s", logicalStorageGroup, virtualStorageGroupId, timePartition, filename, sequence ? "sequence" : "unsequence");
	}
}
