/**
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
package org.apache.iotdb.hive;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.*;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetaData;
import org.apache.iotdb.tsfile.fileSystem.HDFSInput;
import org.apache.iotdb.tsfile.hadoop.TSFInputSplit;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The class implement is same as {@link org.apache.iotdb.tsfile.hadoop.TSFInputFormat}
 * and is customized for Hive to implements JobConfigurable interface.
 */
public class TSFHiveInputFormat extends FileInputFormat<NullWritable, MapWritable> implements JobConfigurable {


  private static final Logger logger = LoggerFactory.getLogger(TSFHiveInputFormat.class);

  // useless now
  private JobConf jobConf;

  @Override
  public void configure(JobConf job) {
    this.jobConf = job;
  }

  @Override
  public RecordReader<NullWritable, MapWritable> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
    return new TSFHiveRecordReader(split, job);
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    BlockLocation[] blockLocations;
    List<InputSplit> splits = new ArrayList<>();
    // get the all file in the directory
    List<FileStatus> listFileStatus = Arrays.asList(super.listStatus(job));
    logger.info("The number of this job file is {}", listFileStatus.size());
    // For each file
    for (FileStatus fileStatus : listFileStatus) {
      logger.info("The file path is {}", fileStatus.getPath());
      // Get the file path
      Path path = fileStatus.getPath();
      // Get the file length
      long length = fileStatus.getLen();
      // Check the file length. if the length is less than 0, return the
      // empty splits
      if (length > 0) {
        FileSystem fileSystem = path.getFileSystem(job);
        logger.info("The file status is {}", fileStatus.getClass().getName());
        logger.info("The file system is " + fileSystem.getClass());
        blockLocations = fileSystem.getFileBlockLocations(fileStatus, 0, length);

        logger.info("The block location information is {}", Arrays.toString(blockLocations));
        try (TsFileSequenceReader fileReader = new TsFileSequenceReader(new HDFSInput(path, job))) {
          splits.addAll(generateSplits(path, fileReader, blockLocations));
        }
      } else {
        logger.warn("The file length is " + length);
      }
    }
    job.setLong(NUM_INPUT_FILES, listFileStatus.size());
    logger.info("The number of splits is " + splits.size());

    return splits.toArray(new InputSplit[0]);
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
  private List<TSFInputSplit> generateSplits(Path path, TsFileSequenceReader fileReader,
                                             BlockLocation[] blockLocations)
          throws IOException {
    List<TSFInputSplit> splits = new ArrayList<>();

    Arrays.sort(blockLocations, Comparator.comparingLong(BlockLocation::getOffset));

    List<ChunkGroupMetaData> chunkGroupMetaDataList = new ArrayList<>();
    int currentBlockIndex = 0;
    long splitSize = 0;
    List<String> hosts = new ArrayList<>();
    for (ChunkGroupMetaData chunkGroupMetaData : fileReader.getSortedChunkGroupMetaDataListByDeviceIds()) {
      logger.info("The chunkGroupMetaData information is {}", chunkGroupMetaData);

      // middle offset point of the chunkGroup
      long middle = (chunkGroupMetaData.getStartOffsetOfChunkGroup() + chunkGroupMetaData.getEndOffsetOfChunkGroup()) / 2;
      int blkIndex = getBlockLocationIndex(blockLocations, middle);
      if (hosts.size() == 0) {
        hosts.addAll(Arrays.asList(blockLocations[blkIndex].getHosts()));
      }

      if (blkIndex != currentBlockIndex) {
        TSFInputSplit tsfInputSplit = makeSplit(path, chunkGroupMetaDataList, splitSize, hosts);
        logger.info("The tsfile inputSplit information is {}", tsfInputSplit);
        splits.add(tsfInputSplit);

        currentBlockIndex = blkIndex;
        chunkGroupMetaDataList.clear();
        chunkGroupMetaDataList.add(chunkGroupMetaData);
        splitSize = getTotalByteSizeOfChunkGroup(chunkGroupMetaData);
        hosts.clear();
      } else {
        chunkGroupMetaDataList.add(chunkGroupMetaData);
        splitSize += getTotalByteSizeOfChunkGroup(chunkGroupMetaData);
      }
    }
    TSFInputSplit tsfInputSplit = makeSplit(path, chunkGroupMetaDataList, splitSize, hosts);
    logger.info("The tsfile inputSplit information is {}", tsfInputSplit);
    splits.add(tsfInputSplit);
    return splits;
  }

  private long getTotalByteSizeOfChunkGroup(ChunkGroupMetaData chunkGroupMetaData) {
    return chunkGroupMetaData.getEndOffsetOfChunkGroup() - chunkGroupMetaData.getStartOffsetOfChunkGroup();
  }


  /**
   * Calculate the index of blockLocation that the chunkGroup belongs to
   * @param blockLocations The Array of blockLocation
   * @param middle Middle offset point of the chunkGroup
   * @return the index of blockLocation or -1 if no block could be found
   */
  private int getBlockLocationIndex(BlockLocation[] blockLocations, long middle) {
    for (int i = 0; i < blockLocations.length; i++) {
      if (blockLocations[i].getOffset() <= middle
              && middle < blockLocations[i].getOffset() + blockLocations[i].getLength()) {
        return i;
      }
    }
    logger.warn(String.format("Can't find the block. The middle is:%d. the last block is", middle),
            blockLocations[blockLocations.length - 1].getOffset()
                    + blockLocations[blockLocations.length - 1].getLength());
    return -1;
  }

  private TSFInputSplit makeSplit(Path path, List<ChunkGroupMetaData> chunkGroupMetaDataList,
                                  long length, List<String> hosts) {
    return new TSFInputSplit(path, hosts.toArray(new String[0]), length,
            chunkGroupMetaDataList.stream()
                    .map(TSFInputSplit.ChunkGroupInfo::new)
                    .collect(Collectors.toList())
    );
  }

}
