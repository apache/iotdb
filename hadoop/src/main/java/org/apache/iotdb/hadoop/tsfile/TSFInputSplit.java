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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * This is tsfile <code>InputSplit</code>.<br>
 * Each <code>InputSplit</code> will be processed by individual
 * <code>Mapper</code> task.
 */
public class TSFInputSplit extends FileSplit implements Writable, org.apache.hadoop.mapred.InputSplit {

  private Path path;

  private String[] hosts;

  private long length;

  private List<ChunkGroupInfo> chunkGroupInfoList;

  public TSFInputSplit() {
    super();
  }

  public TSFInputSplit(Path path, String[] hosts, long length, List<ChunkGroupInfo> chunkGroupInfoList) {
    super(path, 0, length, hosts);
    this.path = path;
    this.hosts = hosts;
    this.length = length;
    this.chunkGroupInfoList = chunkGroupInfoList;
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

  @Override
  public long getLength() {
    return this.length;
  }

  @Override
  public String[] getLocations() throws IOException {
    return this.hosts;
  }

  public List<ChunkGroupInfo> getChunkGroupInfoList() {
    return chunkGroupInfoList;
  }

  public void setChunkGroupInfoList(List<ChunkGroupInfo> chunkGroupInfoList) {
    this.chunkGroupInfoList = chunkGroupInfoList;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(path.toString());
    out.writeLong(length);
    out.writeInt(hosts.length);
    for (String string : hosts) {
      out.writeUTF(string);
    }
    out.writeInt(chunkGroupInfoList.size());
    ChunkGroupInfo.convertToThrift(chunkGroupInfoList, out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    path = new Path(in.readUTF());
    length = in.readLong();
    int len = in.readInt();
    hosts = new String[len];
    for (int i = 0; i < len; i++) {
      hosts[i] = in.readUTF();
    }
    int numOfChunkGroupInfo = in.readInt();
    chunkGroupInfoList = ChunkGroupInfo.readFromThrift(numOfChunkGroupInfo, in);
  }

  @Override
  public String toString() {
    return "TSFInputSplit [path=" + path + ", chunkGroupInfoList=" + chunkGroupInfoList
        + ", length=" + length + ", hosts=" + Arrays.toString(hosts) + "]";
  }


  /**
   * Used to store the key info in chunkGroupMetadata
   */
  public static class ChunkGroupInfo {
    /**
     * Name of device.
     */
    private String deviceId;

    /**
     * Name Of all the sensors in the device.
     */
    private String[] measurementIds;

    /**
     * Byte offset of the corresponding data in the file
     * Notice: include the chunk group marker.
     */
    private long startOffset;

    /**
     * End Byte position of the whole chunk group in the file
     * Notice: position after the chunk group footer.
     */
    private long endOffset;

    public ChunkGroupInfo() {
    }


    public ChunkGroupInfo(String deviceId, String[] measurementIds, long startOffset, long endOffset) {
      this.deviceId = deviceId;
      this.measurementIds = measurementIds;
      this.startOffset = startOffset;
      this.endOffset = endOffset;
    }

    public String getDeviceId() {
      return deviceId;
    }

    public void setDeviceId(String deviceId) {
      this.deviceId = deviceId;
    }

    public String[] getMeasurementIds() {
      return measurementIds;
    }

    public void setMeasurementIds(String[] measurementIds) {
      this.measurementIds = measurementIds;
    }

    public long getStartOffset() {
      return startOffset;
    }

    public void setStartOffset(long startOffset) {
      this.startOffset = startOffset;
    }

    public long getEndOffset() {
      return endOffset;
    }

    public void setEndOffset(long endOffset) {
      this.endOffset = endOffset;
    }

    @Override
    public String toString() {
      return "ChunkGroupInfo{" +
              "deviceId='" + deviceId + '\'' +
              ", measurementIds=" + Arrays.toString(measurementIds) +
              ", startOffset=" + startOffset +
              ", endOffset=" + endOffset +
              '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ChunkGroupInfo that = (ChunkGroupInfo) o;
      return startOffset == that.startOffset &&
              endOffset == that.endOffset &&
              deviceId.equals(that.deviceId) &&
              Arrays.equals(measurementIds, that.measurementIds);
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(deviceId, startOffset, endOffset);
      result = 31 * result + Arrays.hashCode(measurementIds);
      return result;
    }

    /**
     *
     * @param chunkGroupInfoList List of ChunkGroupInfo need to be written into DataOutput
     * @param out DataOutput
     * @throws IOException
     */
    static void convertToThrift(List<ChunkGroupInfo> chunkGroupInfoList, DataOutput out) throws IOException {
      for (ChunkGroupInfo chunkGroupInfo : chunkGroupInfoList) {
        out.writeInt(chunkGroupInfo.deviceId.getBytes(StandardCharsets.UTF_8).length);
        out.write(chunkGroupInfo.deviceId.getBytes(StandardCharsets.UTF_8));
        out.writeInt(chunkGroupInfo.measurementIds.length);
        for (String measurementId : chunkGroupInfo.measurementIds) {
          out.writeInt(measurementId.getBytes(StandardCharsets.UTF_8).length);
          out.write(measurementId.getBytes(StandardCharsets.UTF_8));
        }
        out.writeLong(chunkGroupInfo.startOffset);
        out.writeLong(chunkGroupInfo.endOffset);
      }
    }

    /**
     * Read the specific number of ChunkGroupInfo from DataInput
     * @param num number of ChunkGroupInfo that need to be read
     * @param in DataInput
     * @return The List of ChunkGroupInfo read from DataInput
     * @throws IOException
     */
    static List<ChunkGroupInfo> readFromThrift(int num, DataInput in) throws IOException {
      List<ChunkGroupInfo> chunkGroupInfoList = new ArrayList<>();
      for (int i = 0; i < num; i++) {
        int lenOfDeviceId = in.readInt();
        byte[] b = new byte[lenOfDeviceId];
        in.readFully(b);
        String deviceId = new String(b, StandardCharsets.UTF_8);
        int numOfMeasurement = in.readInt();
        String[] measurementIds = new String[numOfMeasurement];
        for (int j = 0; j < numOfMeasurement; j++) {
          int lenOfMeasurementId = in.readInt();
          byte[] c = new byte[lenOfMeasurementId];
          in.readFully(c);
          measurementIds[j] = new String(c, StandardCharsets.UTF_8);
        }
        long startOffset = in.readLong();
        long endOffset = in.readLong();
        chunkGroupInfoList.add(new ChunkGroupInfo(deviceId, measurementIds, startOffset, endOffset));
      }
      return chunkGroupInfoList;
    }

  }

}
