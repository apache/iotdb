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
package org.apache.iotdb.tsfile.hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetaData;
import org.apache.iotdb.tsfile.file.metadata.RowGroupMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsRowGroupBlockMetaData;
import org.apache.iotdb.tsfile.file.utils.ReadWriteThriftFormatUtils;
import org.apache.iotdb.tsfile.format.RowGroupBlockMetaData;

import java.io.*;
import java.util.Arrays;
import java.util.List;

/**
 * This is tsfile <code>InputSplit</code>.<br>
 * Each <code>InputSplit</code> will be processed by individual
 * <code>Mapper</code> task.
 *
 * @author liukun
 */
public class TSFInputSplit extends InputSplit implements Writable {

  private Path path;
  private int numOfChunkGroup;
  private List<ChunkGroupMetaData> chunkGroupMetaDataList;
  private long start;
  private long length;
  private String[] hosts;

  public TSFInputSplit() {

  }

  /**
   * @param path
   * @param chunkGroupMetaDataList
   * @param start
   * @param length
   * @param hosts
   */
  public TSFInputSplit(Path path, List<ChunkGroupMetaData> chunkGroupMetaDataList, long start,
      long length,
      String[] hosts) {
    this.path = path;
    this.chunkGroupMetaDataList = chunkGroupMetaDataList;
    this.numOfChunkGroup = chunkGroupMetaDataList.size();
    this.start = start;
    this.length = length;
    this.hosts = hosts;
  }

  /**
   * @return the path
   */
  public Path getPath() {
    return path;
  }

  /**
   * @param path the path to set
   */
  public void setPath(Path path) {
    this.path = path;
  }

  /**
   * @return the numOfChunkGroup
   */
  public int getNumOfChunkGroup() {
    return numOfChunkGroup;
  }

  /**
   * @param numOfChunkGroup the numOfChunkGroup to set
   */
  public void setNumOfChunkGroup(int numOfChunkGroup) {
    this.numOfChunkGroup = numOfChunkGroup;
  }

  /**
   * @return the chunkGroupMetaDataList
   */
  public List<ChunkGroupMetaData> getChunkGroupMetaDataList() {
    return chunkGroupMetaDataList;
  }

  /**
   * @param chunkGroupMetaDataList the chunkGroupMetaDataList to set
   */
  public void setChunkGroupMetaDataList(List<ChunkGroupMetaData> chunkGroupMetaDataList) {
    this.chunkGroupMetaDataList = chunkGroupMetaDataList;
  }

  /**
   * @return the start
   */
  public long getStart() {
    return start;
  }

  /**
   * @param start
   *            the start to set
   */
  public void setStart(long start) {
    this.start = start;
  }

  @Override
  public long getLength() throws IOException, InterruptedException {
    return this.length;
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    return this.hosts;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(path.toString());
    out.writeLong(start);
    out.writeLong(length);
    out.writeInt(hosts.length);
    for (int i = 0; i < hosts.length; i++) {
      String string = hosts[i];
      out.writeUTF(string);
    }
    out.writeInt(numOfDeviceRowGroup);
    RowGroupBlockMetaData rowGroupBlockMetaData = new TsRowGroupBlockMetaData(
            chunkGroupMetaDataList)
        .convertToThrift();
    ReadWriteThriftFormatUtils
        .writeRowGroupBlockMetadata(rowGroupBlockMetaData, (OutputStream) out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    path = new Path(in.readUTF());
    this.start = in.readLong();
    this.length = in.readLong();
    int len = in.readInt();
    this.hosts = new String[len];
    for (int i = 0; i < len; i++) {
      hosts[i] = in.readUTF();
    }
    this.numOfDeviceRowGroup = in.readInt();
    TsRowGroupBlockMetaData tsRowGroupBlockMetaData = new TsRowGroupBlockMetaData();
    tsRowGroupBlockMetaData
        .convertToTSF(ReadWriteThriftFormatUtils.readRowGroupBlockMetaData((InputStream) in));
    chunkGroupMetaDataList = tsRowGroupBlockMetaData.getRowGroups();
  }

  @Override
  public String toString() {
    return "TSFInputSplit [path=" + path + ", numOfChunkGroup=" + numOfChunkGroup
        + ", chunkGroupMetaDataList=" + chunkGroupMetaDataList + ", start=" + start
        + ", length="
        + length + ", hosts=" + Arrays.toString(hosts) + "]";
  }

}
