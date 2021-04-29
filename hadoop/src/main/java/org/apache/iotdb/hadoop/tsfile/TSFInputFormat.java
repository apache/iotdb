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
package org.apache.iotdb.hadoop.tsfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

public class TSFInputFormat extends FileInputFormat<NullWritable, MapWritable> {

  /** key to configure whether reading time enable */
  public static final String READ_TIME_ENABLE = "tsfile.read.time.enable";
  /** key to configure whether reading deltaObjectId enable */
  public static final String READ_DELTAOBJECT_ENABLE = "tsfile.read.deltaObjectId.enable";
  /** key to configure the type of filter */
  @Deprecated public static final String FILTER_TYPE = "tsfile.filter.type";
  /** key to configure the filter */
  @Deprecated public static final String FILTER_EXPRESSION = "tsfile.filter.expression";
  /** key to configure whether filtering is enable */
  public static final String FILTER_EXIST = "tsfile.filter.exist";
  /** key to configure the reading deltaObjectIds */
  public static final String READ_DELTAOBJECTS = "tsfile.read.deltaobject";
  /** key to configure the reading measurementIds */
  public static final String READ_MEASUREMENTID = "tsfile.read.measurement";

  private static final Logger logger = LoggerFactory.getLogger(TSFInputFormat.class);
  private static final String SEPARATOR = ",";

  /**
   * Set the deltaObjectIds which want to be read
   *
   * @param job hadoop job
   * @param value the deltaObjectIds will be read
   * @throws TSFHadoopException
   */
  public static void setReadDeviceIds(Job job, String[] value) throws TSFHadoopException {
    if (value == null || value.length < 1) {
      throw new TSFHadoopException("The devices selected is null or empty");
    } else {
      StringBuilder deltaObjectIdsBuilder = new StringBuilder();
      for (String deltaObjectId : value) {
        deltaObjectIdsBuilder.append(deltaObjectId).append(SEPARATOR);
      }
      String deltaObjectIds = deltaObjectIdsBuilder.toString();
      job.getConfiguration()
          .set(
              READ_DELTAOBJECTS,
              (String) deltaObjectIds.subSequence(0, deltaObjectIds.length() - 1));
    }
  }

  /**
   * Get the deltaObjectIds which want to be read
   *
   * @param configuration
   * @return List of device, if configuration has been set the deviceIds. null, if configuration has
   *     not been set the deviceIds.
   */
  public static List<String> getReadDeviceIds(Configuration configuration) {
    String deviceIds = configuration.get(READ_DELTAOBJECTS);
    if (deviceIds == null || deviceIds.length() < 1) {
      return new LinkedList<>();
    } else {

      return Arrays.stream(deviceIds.split(SEPARATOR)).collect(Collectors.toList());
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
      StringBuilder measurementIdsBuilder = new StringBuilder();
      for (String measurementId : value) {
        measurementIdsBuilder.append(measurementId).append(SEPARATOR);
      }
      String measurementIds = measurementIdsBuilder.toString();
      // Get conf type
      job.getConfiguration()
          .set(
              READ_MEASUREMENTID,
              (String) measurementIds.subSequence(0, measurementIds.length() - 1));
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
      return new LinkedList<>();
    } else {
      return Arrays.stream(measurementIds.split(SEPARATOR)).collect(Collectors.toList());
    }
  }

  /**
   * @param job
   * @param value
   */
  public static void setReadDeviceId(Job job, boolean value) {
    job.getConfiguration().setBoolean(READ_DELTAOBJECT_ENABLE, value);
  }

  /**
   * @param configuration
   * @return
   */
  public static boolean getReadDeviceId(Configuration configuration) {
    return configuration.getBoolean(READ_DELTAOBJECT_ENABLE, true);
  }

  /**
   * @param job
   * @param value
   */
  public static void setReadTime(Job job, boolean value) {
    job.getConfiguration().setBoolean(READ_TIME_ENABLE, value);
  }

  public static boolean getReadTime(Configuration configuration) {
    return configuration.getBoolean(READ_TIME_ENABLE, true);
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
  public RecordReader<NullWritable, MapWritable> createRecordReader(
      InputSplit split, TaskAttemptContext context) {
    return new TSFRecordReader();
  }

  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    job.getConfiguration().setBoolean(INPUT_DIR_RECURSIVE, true);
    List<FileStatus> listFileStatus = super.listStatus(job);
    return new ArrayList<>(getTSFInputSplit(job.getConfiguration(), listFileStatus, logger));
  }

  public static List<TSFInputSplit> getTSFInputSplit(
      Configuration configuration, List<FileStatus> listFileStatus, Logger logger)
      throws IOException {
    BlockLocation[] blockLocations;
    List<TSFInputSplit> splits = new ArrayList<>();
    // get the all file in the directory
    logger.info("The number of this job file is {}", listFileStatus.size());
    // For each file
    for (FileStatus fileStatus : listFileStatus) {
      logger.info("The file path is {}", fileStatus.getPath());
      // Get the file path
      Path path = fileStatus.getPath();
      if (!path.toString().endsWith(TSFILE_SUFFIX)) {
        continue;
      }
      // Get the file length
      long length = fileStatus.getLen();
      // Check the file length. if the length is less than 0, return the
      // empty splits
      if (length > 0) {
        FileSystem fileSystem = path.getFileSystem(configuration);
        logger.info("The file status is {}", fileStatus.getClass().getName());
        logger.info("The file system is " + fileSystem.getClass());
        blockLocations = fileSystem.getFileBlockLocations(fileStatus, 0, length);

        logger.info("The block location information is {}", Arrays.toString(blockLocations));
        splits.addAll(generateSplits(path, blockLocations));
      } else {
        logger.warn("The file length is " + length);
      }
    }
    configuration.setLong(NUM_INPUT_FILES, listFileStatus.size());
    logger.info("The number of splits is " + splits.size());

    return splits;
  }

  /**
   * get the TSFInputSplit from tsfMetaData and hdfs block location information with the filter
   *
   * @throws IOException
   */
  private static List<TSFInputSplit> generateSplits(Path path, BlockLocation[] blockLocations)
      throws IOException {
    List<TSFInputSplit> splits = new ArrayList<>();
    for (BlockLocation blockLocation : blockLocations) {
      splits.add(
          new TSFInputSplit(
              path,
              blockLocation.getHosts(),
              blockLocation.getOffset(),
              blockLocation.getLength()));
    }
    return splits;
  }
}
