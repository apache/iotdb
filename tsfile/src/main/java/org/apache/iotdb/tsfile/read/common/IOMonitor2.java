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

package org.apache.iotdb.tsfile.read.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IOMonitor2 {
  public enum DataSetType { // dataSet, executor, reader, file
    NONE,
    RawQueryDataSetWithoutValueFilter,
    UDTFAlignByTimeDataSet,
    GroupByWithoutValueFilterDataSet
  }

  public enum Operation {
    DCP_Server_Query_Execute("DCP_Server_Query_Execute"),
    DCP_Server_Query_Fetch("DCP_Server_Query_Fetch"),
    DCP_SeriesScanOperator_hasNext("DCP_SeriesScanOperator_hasNext"),
    DCP_A_GET_CHUNK_METADATAS("DCP_A_GET_CHUNK_METADATAS"),
    DCP_B_READ_MEM_CHUNK("DCP_B_READ_MEM_CHUNK"),
    DCP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA(
        "DCP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA"),
    DCP_D_DECODE_PAGEDATA_TRAVERSE_POINTS("DCP_D_DECODE_PAGEDATA_TRAVERSE_POINTS");

    public String getName() {
      return name;
    }

    String name;

    Operation(String name) {
      this.name = name;
    }
  }

  /**
   * [Raw data query metrics] - server端execute&fetch的次数和耗时 -
   * SeriesScanOperator.hasNext的次数和耗时（问题：root.hasNext执行次数等于decode
   * page次数，但是SeriesScanOperator.hasNext次数大于那个次数？破案了：是root.isFinished->SeriesScanOperator.isFinished里面调用了hasNext，和root.hasNext里调用SeriesScanOperator.hasNext的次数加起来就对了。）
   * - A：从磁盘解读chunk元数据的次数和耗时 - B：从磁盘加载chunk数据的次数和耗时 - C：解压缩所有page构造pageReaderList的次数和耗时 -
   * D：解码page并遍历点的次数和耗时 （遍历点数不用统计，因为一定是全量点）
   */
  public static int DCP_Server_Query_Execute_count = 0;

  public static long DCP_Server_Query_Execute_ns = 0;

  public static int DCP_Server_Query_Fetch_count = 0;
  public static long DCP_Server_Query_Fetch_ns = 0;

  public static int DCP_SeriesScanOperator_hasNext_count = 0;
  public static long DCP_SeriesScanOperator_hasNext_ns = 0;

  public static int DCP_A_GET_CHUNK_METADATAS_count = 0;
  public static long DCP_A_GET_CHUNK_METADATAS_ns = 0;

  public static int DCP_B_READ_MEM_CHUNK_count = 0;
  public static long DCP_B_READ_MEM_CHUNK_ns = 0;

  public static int DCP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA_count = 0;
  public static long DCP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA_ns = 0;

  public static int DCP_D_DECODE_PAGEDATA_TRAVERSE_POINTS_count = 0;
  public static long DCP_D_DECODE_PAGEDATA_TRAVERSE_POINTS_ns = 0;

  public static long DCP_D_traversedPointNum = 0;

  public static DataSetType dataSetType = DataSetType.NONE;

  private static final Logger DEBUG_LOGGER = LoggerFactory.getLogger("IOMonitor2");

  private static void reset() {
    DCP_Server_Query_Execute_count = 0;
    DCP_Server_Query_Execute_ns = 0;

    DCP_Server_Query_Fetch_count = 0;
    DCP_Server_Query_Fetch_ns = 0;

    DCP_SeriesScanOperator_hasNext_count = 0;
    DCP_SeriesScanOperator_hasNext_ns = 0;

    DCP_A_GET_CHUNK_METADATAS_count = 0;
    DCP_A_GET_CHUNK_METADATAS_ns = 0;

    DCP_B_READ_MEM_CHUNK_count = 0;
    DCP_B_READ_MEM_CHUNK_ns = 0;

    DCP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA_count = 0;
    DCP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA_ns = 0;

    DCP_D_DECODE_PAGEDATA_TRAVERSE_POINTS_count = 0;
    DCP_D_DECODE_PAGEDATA_TRAVERSE_POINTS_ns = 0;

    DCP_D_traversedPointNum = 0;

    dataSetType = DataSetType.NONE;
  }

  public static void addMeasure(Operation operation, long elapsedTimeInNanosecond) {
    // TODO tmp for debug
    // DEBUG_LOGGER.info(operation.getName() + ": " + elapsedTimeInNanosecond + " ns");
    switch (operation) {
      case DCP_Server_Query_Execute:
        DCP_Server_Query_Execute_count++;
        DCP_Server_Query_Execute_ns += elapsedTimeInNanosecond;
        break;
      case DCP_Server_Query_Fetch:
        DCP_Server_Query_Fetch_count++;
        DCP_Server_Query_Fetch_ns += elapsedTimeInNanosecond;
        break;
      case DCP_SeriesScanOperator_hasNext:
        DCP_SeriesScanOperator_hasNext_count++;
        DCP_SeriesScanOperator_hasNext_ns += elapsedTimeInNanosecond;
        break;
      case DCP_A_GET_CHUNK_METADATAS:
        DCP_A_GET_CHUNK_METADATAS_count++;
        DCP_A_GET_CHUNK_METADATAS_ns += elapsedTimeInNanosecond;
        break;
      case DCP_B_READ_MEM_CHUNK:
        DCP_B_READ_MEM_CHUNK_count++;
        DCP_B_READ_MEM_CHUNK_ns += elapsedTimeInNanosecond;
        break;
      case DCP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA:
        DCP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA_count++;
        DCP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA_ns += elapsedTimeInNanosecond;
        break;
      case DCP_D_DECODE_PAGEDATA_TRAVERSE_POINTS:
        DCP_D_DECODE_PAGEDATA_TRAVERSE_POINTS_count++;
        DCP_D_DECODE_PAGEDATA_TRAVERSE_POINTS_ns += elapsedTimeInNanosecond;
        break;
      default:
        System.out.println("not supported operation type"); // this will not happen
        break;
    }
  }

  public static void addMeasure(Operation operation, long elapsedTimeInNanosecond, int count) {
    switch (operation) {
      case DCP_Server_Query_Execute:
        DCP_Server_Query_Execute_count += count;
        DCP_Server_Query_Execute_ns += elapsedTimeInNanosecond;
        break;
      case DCP_Server_Query_Fetch:
        DCP_Server_Query_Fetch_count += count;
        DCP_Server_Query_Fetch_ns += elapsedTimeInNanosecond;
        break;
      case DCP_SeriesScanOperator_hasNext:
        DCP_SeriesScanOperator_hasNext_count += count;
        DCP_SeriesScanOperator_hasNext_ns += elapsedTimeInNanosecond;
        break;
      case DCP_A_GET_CHUNK_METADATAS:
        DCP_A_GET_CHUNK_METADATAS_count += count;
        DCP_A_GET_CHUNK_METADATAS_ns += elapsedTimeInNanosecond;
        break;
      case DCP_B_READ_MEM_CHUNK:
        DCP_B_READ_MEM_CHUNK_count += count;
        DCP_B_READ_MEM_CHUNK_ns += elapsedTimeInNanosecond;
        break;
      case DCP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA:
        DCP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA_count += count;
        DCP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA_ns += elapsedTimeInNanosecond;
        break;
      case DCP_D_DECODE_PAGEDATA_TRAVERSE_POINTS:
        DCP_D_DECODE_PAGEDATA_TRAVERSE_POINTS_count += count;
        DCP_D_DECODE_PAGEDATA_TRAVERSE_POINTS_ns += elapsedTimeInNanosecond;
        break;
      default:
        System.out.println("not supported operation type"); // this will not happen
        break;
    }
  }

  public static String print() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("dataSetType").append(",").append(dataSetType).append("\n");

    stringBuilder
        .append("Server_Query_Execute_ns")
        .append(",")
        .append(DCP_Server_Query_Execute_ns)
        .append("\n");
    stringBuilder
        .append("Server_Query_Fetch_ns")
        .append(",")
        .append(DCP_Server_Query_Fetch_ns)
        .append("\n");
    stringBuilder
        .append("SeriesScanOperator_hasNext_ns")
        .append(",")
        .append(DCP_SeriesScanOperator_hasNext_ns)
        .append("\n");
    stringBuilder.append("A_ns").append(",").append(DCP_A_GET_CHUNK_METADATAS_ns).append("\n");
    stringBuilder.append("B_ns").append(",").append(DCP_B_READ_MEM_CHUNK_ns).append("\n");
    stringBuilder
        .append("C_ns")
        .append(",")
        .append(DCP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA_ns)
        .append("\n");
    stringBuilder
        .append("D_ns")
        .append(",")
        .append(DCP_D_DECODE_PAGEDATA_TRAVERSE_POINTS_ns)
        .append("\n");

    stringBuilder
        .append("Server_Query_Execute_cnt")
        .append(",")
        .append(DCP_Server_Query_Execute_count)
        .append("\n");
    stringBuilder
        .append("Server_Query_Fetch_cnt")
        .append(",")
        .append(DCP_Server_Query_Fetch_count)
        .append("\n");
    stringBuilder
        .append("SeriesScanOperator_hasNext_cnt")
        .append(",")
        .append(DCP_SeriesScanOperator_hasNext_count)
        .append("\n");
    stringBuilder.append("A_cnt").append(",").append(DCP_A_GET_CHUNK_METADATAS_count).append("\n");
    stringBuilder.append("B_cnt").append(",").append(DCP_B_READ_MEM_CHUNK_count).append("\n");
    stringBuilder
        .append("C_cnt")
        .append(",")
        .append(DCP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA_count)
        .append("\n");
    stringBuilder
        .append("D_cnt")
        .append(",")
        .append(DCP_D_DECODE_PAGEDATA_TRAVERSE_POINTS_count)
        .append("\n");
    stringBuilder
        .append("DCP_D_traversedPointNum")
        .append(",")
        .append(DCP_D_traversedPointNum)
        .append("\n");

    reset(); // whenever print() is called, reset the metrics, to clean warm up information.
    return stringBuilder.toString();
  }
}
