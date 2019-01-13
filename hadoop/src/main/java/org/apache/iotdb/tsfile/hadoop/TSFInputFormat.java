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
package org.apache.iotdb.tsfile.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.iotdb.tsfile.hadoop.io.HDFSInputStream;
import org.apache.iotdb.tsfile.file.metadata.RowGroupMetaData;
import org.apache.iotdb.tsfile.read.FileReader;

/**
 * @author liukun
 */
public class TSFInputFormat extends FileInputFormat<NullWritable, ArrayWritable> {

	private static final Logger LOGGER = LoggerFactory.getLogger(TSFInputFormat.class);

	/**
	 * key to configure whether reading time enable
	 */
	public static final String READ_TIME_ENABLE = "tsfile.read.time.enable";
	/**
	 * key to configure whether reading deltaObjectId enable
	 */
	public static final String READ_DELTAOBJECT_ENABLE = "tsfile.read.deltaObjectId.enable";
	/**
	 * key to configure the type of filter
	 */
	@Deprecated
	public static final String FILTER_TYPE = "tsfile.filter.type";
	/**
	 * key to configure the filter
	 */
	@Deprecated
	public static final String FILTER_EXPRESSION = "tsfile.filter.expression";
	/**
	 * key to configure whether filtering is enable 	
	 */
	public static final String FILTER_EXIST = "tsfile.filter.exist";
	/**
	 * key to configure the reading deltaObjectIds
	 */
	public static final String READ_DELTAOBJECTS = "tsfile.read.deltaobject";
	/**
	 * key to configure the reading measurementIds
	 */
	public static final String READ_MEASUREMENTID = "tsfile.read.measurement";

	private static final String SPERATOR = ",";

	/**
	 * Set the deltaObjectIds which want to be read
	 *
	 * @param job hadoop job
	 * @param value the deltaObjectIds will be read
	 * @throws TSFHadoopException
	 */
	public static void setReadDeltaObjectIds(Job job, String[] value) throws TSFHadoopException {
		if (value == null || value.length < 1) {
			throw new TSFHadoopException("The devices selected is null or empty");
		} else {
			String deltaObjectIds = "";
			for (String deltaObjectId : value) {
				deltaObjectIds = deltaObjectIds + deltaObjectId + SPERATOR;
			}
			job.getConfiguration().set(READ_DELTAOBJECTS, (String) deltaObjectIds.subSequence(0, deltaObjectIds.length() - 1));
		}
	}

	/**
	 * Get the deltaObjectIds which want to be read
	 *
	 * @param configuration
	 * @return List of deltaObject, if configuration has been set the deltaObjectIds.
	 * 		   null, if configuration has not been set the deltaObjectIds. 
	 */
	public static List<String> getReadDeltaObjectIds(Configuration configuration) {
		String deltaObjectIds = configuration.get(READ_DELTAOBJECTS);
		if (deltaObjectIds == null || deltaObjectIds.length() < 1) {
			return null;
		} else {
			List<String> deltaObjectIdsList = Arrays.asList(deltaObjectIds.split(SPERATOR));
			return deltaObjectIdsList;
		}
	}

	/**
	 * Set the measurementIds which want to be read
	 *
	 * @param job hadoop job
	 * @param value the measurementIds will be read
	 * @throws TSFHadoopException
	 */
	public static void setReadMeasurementIds(Job job, String[] value) throws TSFHadoopException {
		if (value == null || value.length < 1) {
			throw new TSFHadoopException("The sensors selected is null or empty");
		} else {
			String measurementIds = "";
			for (String measurementId : value) {
				measurementIds = measurementIds + measurementId + SPERATOR;
			}
			// Get conf type
			job.getConfiguration().set(READ_MEASUREMENTID, (String) measurementIds.subSequence(0, measurementIds.length() - 1));
		}
	}

	/**
	 * Get the measurementIds which want to be read
	 *
	 * @param configuration hadoop configuration
	 * @return if not set the measurementIds, return null
	 */
	public static List<String> getReadMeasurementIds(Configuration configuration) {
		String measurementIds = configuration.get(READ_MEASUREMENTID);
		if (measurementIds == null || measurementIds.length() < 1) {
			return null;
		} else {
			List<String> measurementIdsList = Arrays.asList(measurementIds.split(SPERATOR));
			return measurementIdsList;
		}
	}

	/**
	 * @param job
	 * @param value
	 */
	public static void setReadDeltaObjectId(Job job, boolean value) {
		job.getConfiguration().setBoolean(READ_DELTAOBJECT_ENABLE, value);
	}

	/**
	 * @param configuration
	 * @return
	 */
	public static boolean getReadDeltaObject(Configuration configuration) {
		return configuration.getBoolean(READ_DELTAOBJECT_ENABLE, false);
	}

	/**
	 * @param job
	 * @param value
	 */
	public static void setReadTime(Job job, boolean value) {
		job.getConfiguration().setBoolean(READ_TIME_ENABLE, value);
	}

	public static boolean getReadTime(Configuration configuration) {
		return configuration.getBoolean(READ_TIME_ENABLE, false);
	}

	/**
	 * Set filter exist or not
	 *
	 * @param job
	 * @param value
	 */
	@Deprecated
	public static void setHasFilter(Job job, boolean value) {
		job.getConfiguration().setBoolean(FILTER_EXIST, value);
	}

	// check is we didn't set this key, the value will be null or empty

	/**
	 * Get filter exist or not
	 *
	 * @param configuration
	 * @return
	 */
	@Deprecated
	public static boolean getHasFilter(Configuration configuration) {
		return configuration.getBoolean(FILTER_EXIST, false);
	}

	/**
	 * @param job
	 * @param value
	 */
	@Deprecated
	public static void setFilterType(Job job, String value) {
		job.getConfiguration().set(FILTER_TYPE, value);
	}

	/**
	 * Get the filter type
	 *
	 * @param configuration
	 * @return
	 */
	// check if not set the filter type, the result will null or empty
	@Deprecated
	public static String getFilterType(Configuration configuration) {
		return configuration.get(FILTER_TYPE);
	}
	@Deprecated
	public static void setFilterExp(Job job, String value) {
		job.getConfiguration().set(FILTER_EXPRESSION, value);
	}
	@Deprecated
	public static String getFilterExp(Configuration configuration) {
		return configuration.get(FILTER_EXPRESSION);
	}

	@Override
	public RecordReader<NullWritable, ArrayWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new TSFRecordReader();
	}

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		Configuration configuration = job.getConfiguration();
		BlockLocation[] blockLocations;
		List<InputSplit> splits = new ArrayList<>();
		// get the all file in the directory
		List<FileStatus> listFileStatus = super.listStatus(job);
		LOGGER.info("The number of this job file is {}", listFileStatus.size());
		// For each file
		for (FileStatus fileStatus : listFileStatus) {
			LOGGER.info("The file path is {}", fileStatus.getPath());
			// Get the file path
			Path path = fileStatus.getPath();
			// Get the file length
			long length = fileStatus.getLen();
			// Check the file length. if the length is less than 0, return the
			// empty splits
			if (length > 0) {
				// Get block information in the local file system or hdfs
				if (fileStatus instanceof LocatedFileStatus) {
					LOGGER.info("The file status is {}", LocatedFileStatus.class.getName());
					blockLocations = ((LocatedFileStatus) fileStatus).getBlockLocations();
				} else {
					FileSystem fileSystem = path.getFileSystem(configuration);
					LOGGER.info("The file status is {}", fileStatus.getClass().getName());
					System.out.println("The file status is " + fileStatus.getClass().getName());
					System.out.println("The file system is " + fileSystem.getClass());
					blockLocations = fileSystem.getFileBlockLocations(fileStatus, 0, length);
				}
				LOGGER.info("The block location information is {}", Arrays.toString(blockLocations));
				HDFSInputStream hdfsInputStream = new HDFSInputStream(path, configuration);
				FileReader fileReader = new FileReader(hdfsInputStream);
				// Get the timeserise to test
				splits.addAll(generateSplits(path, fileReader, blockLocations));
				fileReader.close();
			} else {
				LOGGER.warn("The file length is " + length);
			}
		}
		configuration.setLong(NUM_INPUT_FILES, listFileStatus.size());
		LOGGER.info("The number of splits is " + splits.size());

		return splits;
	}

	/**
	 * get the TSFInputSplit from tsfMetaData and hdfs block location
	 * information with the filter
	 *
	 * @param path
	 * @param fileReader
	 * @param blockLocations
	 * @return
	 * @throws IOException
	 */
	private List<TSFInputSplit> generateSplits(Path path, FileReader fileReader, BlockLocation[] blockLocations)
			throws IOException {
		List<TSFInputSplit> splits = new ArrayList<TSFInputSplit>();
		Comparator<BlockLocation> comparator = new Comparator<BlockLocation>() {
			@Override
			public int compare(BlockLocation o1, BlockLocation o2) {

				return Long.signum(o1.getOffset() - o2.getOffset());
			}

		};
		Arrays.sort(blockLocations, comparator);

		List<RowGroupMetaData> rowGroupMetaDataList = new ArrayList<>();
		int currentBlockIndex = 0;
		long splitSize = 0;
		long splitStart = 0;
		List<String> hosts = new ArrayList<>();
		for (RowGroupMetaData rowGroupMetaData : fileReader.getSortedRowGroupMetaDataList()) {
			LOGGER.info("The rowGroupMetaData information is {}", rowGroupMetaData);

			long start = getRowGroupStart(rowGroupMetaData);
			int blkIndex = getBlockLocationIndex(blockLocations, start);
			if(hosts.size() == 0)
			{
				hosts.addAll(Arrays.asList(blockLocations[blkIndex].getHosts()));
				splitStart = start;
			}

			if(blkIndex != currentBlockIndex)
			{
				TSFInputSplit tsfInputSplit = makeSplit(path, rowGroupMetaDataList, splitStart,
						splitSize, hosts);
				LOGGER.info("The tsfile inputsplit information is {}", tsfInputSplit);
				splits.add(tsfInputSplit);

				currentBlockIndex = blkIndex;
				rowGroupMetaDataList.clear();
				rowGroupMetaDataList.add(rowGroupMetaData);
				splitStart = start;
				splitSize = rowGroupMetaData.getTotalByteSize();
				hosts.clear();
			}
			else
			{
				rowGroupMetaDataList.add(rowGroupMetaData);
				splitSize += rowGroupMetaData.getTotalByteSize();
			}
		}
		TSFInputSplit tsfInputSplit = makeSplit(path, rowGroupMetaDataList, splitStart,
				splitSize, hosts);
		LOGGER.info("The tsfile inputsplit information is {}", tsfInputSplit);
		splits.add(tsfInputSplit);
		return splits;
	}

	private long getRowGroupStart(RowGroupMetaData rowGroupMetaData) {
		return rowGroupMetaData.getMetaDatas().get(0).getProperties().getFileOffset();
	}

	private int getBlockLocationIndex(BlockLocation[] blockLocations, long start) {
		for (int i = 0; i < blockLocations.length; i++) {
			if (blockLocations[i].getOffset() <= start
					&& start < blockLocations[i].getOffset() + blockLocations[i].getLength()) {
				return i;
			}
		}
		LOGGER.warn(String.format("Can't find the block. The start is:%d. the last block is", start),
				blockLocations[blockLocations.length - 1].getOffset()
						+ blockLocations[blockLocations.length - 1].getLength());
		return -1;
	}

	private TSFInputSplit makeSplit(Path path, List<RowGroupMetaData> rowGroupMataDataList, long start, long length,
			List<String> hosts) {
		String[] hosts_str = hosts.toArray(new String[hosts.size()]);
		return new TSFInputSplit(path, rowGroupMataDataList, start, length, hosts_str);
	}
}
