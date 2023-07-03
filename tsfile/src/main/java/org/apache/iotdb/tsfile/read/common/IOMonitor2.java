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

public class IOMonitor2 {

  public enum DataSetType { // dataSet, executor, reader, file
    NONE,
    RawQueryDataSetWithoutValueFilter,
    UDTFAlignByTimeDataSet_M4_POINT, // mac
    UDTFAlignByTimeDataSet_M4_TIMEWINDOW, // mac_tw
    GroupByWithoutValueFilterDataSet_LocalGroupByExecutor4CPV_UseIndex, // cpv
    GroupByWithoutValueFilterDataSet_LocalGroupByExecutor4CPV_NotUseIndex, // cpv_noIndex

    GroupByWithoutValueFilterDataSet_LocalGroupByExecutor_UseStatistics, // moc
    GroupByWithoutValueFilterDataSet_LocalGroupByExecutor_NotUseStatistics // mac_groupBy
  }

  public enum Operation {
    DCP_Server_Query_Execute("DCP_Server_Query_Execute"),
    DCP_Server_Query_Fetch("DCP_Server_Query_Fetch"),
    DCP_A_GET_CHUNK_METADATAS("DCP_A_GET_CHUNK_METADATAS"),
    DCP_B_READ_MEM_CHUNK("DCP_B_READ_MEM_CHUNK"),
    DCP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA(
        "DCP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA"),
    DCP_D_DECODE_PAGEDATA_TRAVERSE_POINTS("DCP_D_DECODE_PAGEDATA_TRAVERSE_POINTS"),
    SEARCH_ARRAY_a_verifBPTP("SEARCH_ARRAY_a_verifBPTP"),
    SEARCH_ARRAY_b_genFP("SEARCH_ARRAY_b_genFP"),
    SEARCH_ARRAY_b_genLP("SEARCH_ARRAY_b_genLP"),
    SEARCH_ARRAY_c_genBPTP("SEARCH_ARRAY_c_genBPTP"),
    M4_LSM_INIT_LOAD_ALL_CHUNKMETADATAS("M4_LSM_INIT_LOAD_ALL_CHUNKMETADATAS"),
    M4_LSM_MERGE_M4_TIME_SPAN("M4_LSM_MERGE_M4_TIME_SPAN"),
    M4_LSM_FP("M4_LSM_FP"),
    M4_LSM_LP("M4_LSM_LP"),
    M4_LSM_BP("M4_LSM_BP"),
    M4_LSM_TP("M4_LSM_TP");

    public String getName() {
      return name;
    }

    String name;

    Operation(String name) {
      this.name = name;
    }
  }

  public static int DCP_Server_Query_Execute_count = 0; // level 2
  public static long DCP_Server_Query_Execute_ns = 0; // level 2

  public static int DCP_Server_Query_Fetch_count = 0; // level 2
  public static long DCP_Server_Query_Fetch_ns = 0; // level 2

  public static DataSetType dataSetType = DataSetType.NONE; // level 3

  public static int M4_LSM_init_loadAllChunkMetadatas_count = 0; // level 3
  public static long M4_LSM_init_loadAllChunkMetadatas_ns = 0; // level 3

  public static int M4_LSM_merge_M4_time_span_count = 0; // level 3
  public static long M4_LSM_merge_M4_time_span_ns = 0; // level 3

  public static int M4_LSM_FP_count = 0; // level 3
  public static long M4_LSM_FP_ns = 0; // level 3

  public static int M4_LSM_LP_count = 0; // level 3
  public static long M4_LSM_LP_ns = 0; // level 3

  public static int M4_LSM_BP_count = 0; // level 3
  public static long M4_LSM_BP_ns = 0; // level 3

  public static int M4_LSM_TP_count = 0; // level 3
  public static long M4_LSM_TP_ns = 0; // level 3

  public static int DCP_A_GET_CHUNK_METADATAS_count = 0; // level 4
  public static long DCP_A_GET_CHUNK_METADATAS_ns = 0; // level 4

  public static int DCP_B_READ_MEM_CHUNK_count = 0; // level 4
  public static long DCP_B_READ_MEM_CHUNK_ns = 0; // level 4

  public static int DCP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA_count = 0; // level 4
  public static long DCP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA_ns = 0; // level 4

  public static long DCP_D_traversedPointNum = 0; // level 4
  public static int DCP_D_DECODE_PAGEDATA_TRAVERSE_POINTS_count = 0; // level 4
  public static long DCP_D_DECODE_PAGEDATA_TRAVERSE_POINTS_ns = 0; // level 4

  public static int SEARCH_ARRAY_a_verifBPTP_count = 0; // level 4
  public static long SEARCH_ARRAY_a_verifBPTP_ns = 0; // level 4

  public static int SEARCH_ARRAY_b_genFP_count = 0; // level 4
  public static long SEARCH_ARRAY_b_genFP_ns = 0; // level 4

  public static int SEARCH_ARRAY_b_genLP_count = 0; // level 4
  public static long SEARCH_ARRAY_b_genLP_ns = 0; // level 4

  public static int SEARCH_ARRAY_c_genBPTP_count = 0; // level 4
  public static long SEARCH_ARRAY_c_genBPTP_ns = 0; // level 4

  public static Operation M4_LSM_status =
      null; // for counting the number of calling BCD by each step of M4-LSM
  public static int M4_LSM_merge_M4_time_span_B_READ_MEM_CHUNK_cnt = 0; // map from level 3 to 4
  public static int M4_LSM_merge_M4_time_span_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA_cnt =
      0; // map from level 3 to 4
  public static int M4_LSM_merge_M4_time_span_SEARCH_ARRAY_a_verifBPTP_cnt =
      0; // map from level 3 to 4
  public static int M4_LSM_merge_M4_time_span_SEARCH_ARRAY_b_genFP_cnt = 0; // map from level 3 to 4
  public static int M4_LSM_merge_M4_time_span_SEARCH_ARRAY_b_genLP_cnt = 0; // map from level 3 to 4
  public static int M4_LSM_merge_M4_time_span_SEARCH_ARRAY_c_genBPTP_cnt =
      0; // map from level 3 to 4
  public static int M4_LSM_FP_B_READ_MEM_CHUNK_cnt = 0; // map from level 3 to 4
  public static int M4_LSM_FP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA_cnt =
      0; // map from level 3 to 4
  public static int M4_LSM_FP_SEARCH_ARRAY_a_verifBPTP_cnt = 0; // map from level 3 to 4
  public static int M4_LSM_FP_SEARCH_ARRAY_b_genFP_cnt = 0; // map from level 3 to 4
  public static int M4_LSM_FP_SEARCH_ARRAY_b_genLP_cnt = 0; // map from level 3 to 4
  public static int M4_LSM_FP_SEARCH_ARRAY_c_genBPTP_cnt = 0; // map from level 3 to 4
  public static int M4_LSM_LP_B_READ_MEM_CHUNK_cnt = 0; // map from level 3 to 4
  public static int M4_LSM_LP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA_cnt =
      0; // map from level 3 to 4
  public static int M4_LSM_LP_SEARCH_ARRAY_a_verifBPTP_cnt = 0; // map from level 3 to 4
  public static int M4_LSM_LP_SEARCH_ARRAY_b_genFP_cnt = 0; // map from level 3 to 4
  public static int M4_LSM_LP_SEARCH_ARRAY_b_genLP_cnt = 0; // map from level 3 to 4
  public static int M4_LSM_LP_SEARCH_ARRAY_c_genBPTP_cnt = 0; // map from level 3 to 4
  public static int M4_LSM_BP_B_READ_MEM_CHUNK_cnt = 0; // map from level 3 to 4
  public static int M4_LSM_BP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA_cnt =
      0; // map from level 3 to 4
  public static int M4_LSM_BP_SEARCH_ARRAY_a_verifBPTP_cnt = 0; // map from level 3 to 4
  public static int M4_LSM_BP_SEARCH_ARRAY_b_genFP_cnt = 0; // map from level 3 to 4
  public static int M4_LSM_BP_SEARCH_ARRAY_b_genLP_cnt = 0; // map from level 3 to 4
  public static int M4_LSM_BP_SEARCH_ARRAY_c_genBPTP_cnt = 0; // map from level 3 to 4
  public static int M4_LSM_TP_B_READ_MEM_CHUNK_cnt = 0; // map from level 3 to 4
  public static int M4_LSM_TP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA_cnt =
      0; // map from level 3 to 4
  public static int M4_LSM_TP_SEARCH_ARRAY_a_verifBPTP_cnt = 0; // map from level 3 to 4
  public static int M4_LSM_TP_SEARCH_ARRAY_b_genFP_cnt = 0; // map from level 3 to 4
  public static int M4_LSM_TP_SEARCH_ARRAY_b_genLP_cnt = 0; // map from level 3 to 4
  public static int M4_LSM_TP_SEARCH_ARRAY_c_genBPTP_cnt = 0; // map from level 3 to 4

  private static void reset() {
    // level 1 is client elapsed time, not measured by the server side
    /** level 2: server execute & fetch */
    DCP_Server_Query_Execute_count = 0;
    DCP_Server_Query_Execute_ns = 0;

    DCP_Server_Query_Fetch_count = 0;
    DCP_Server_Query_Fetch_ns = 0;

    /** level 3 */
    dataSetType = DataSetType.NONE;

    M4_LSM_init_loadAllChunkMetadatas_count = 0;
    M4_LSM_init_loadAllChunkMetadatas_ns = 0;

    M4_LSM_merge_M4_time_span_count = 0;
    M4_LSM_merge_M4_time_span_ns = 0;

    M4_LSM_FP_count = 0;
    M4_LSM_FP_ns = 0;

    M4_LSM_LP_count = 0;
    M4_LSM_LP_ns = 0;

    M4_LSM_BP_count = 0;
    M4_LSM_BP_ns = 0;

    M4_LSM_TP_count = 0;
    M4_LSM_TP_ns = 0;

    /** level 4 */
    DCP_A_GET_CHUNK_METADATAS_count = 0;
    DCP_A_GET_CHUNK_METADATAS_ns = 0;

    DCP_B_READ_MEM_CHUNK_count = 0;
    DCP_B_READ_MEM_CHUNK_ns = 0;

    DCP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA_count = 0;
    DCP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA_ns = 0;

    DCP_D_traversedPointNum = 0;

    DCP_D_DECODE_PAGEDATA_TRAVERSE_POINTS_count = 0;
    DCP_D_DECODE_PAGEDATA_TRAVERSE_POINTS_ns = 0;

    SEARCH_ARRAY_a_verifBPTP_count = 0;
    SEARCH_ARRAY_a_verifBPTP_ns = 0;
    SEARCH_ARRAY_b_genFP_count = 0;
    SEARCH_ARRAY_b_genFP_ns = 0;
    SEARCH_ARRAY_b_genLP_count = 0;
    SEARCH_ARRAY_b_genLP_ns = 0;
    SEARCH_ARRAY_c_genBPTP_count = 0;
    SEARCH_ARRAY_c_genBPTP_ns = 0;

    /** map from level 3 to level 4 */
    M4_LSM_status = null;
    M4_LSM_merge_M4_time_span_B_READ_MEM_CHUNK_cnt =
        0; // for counting the number of calling BCD by each step of M4-LSM
    M4_LSM_merge_M4_time_span_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA_cnt = 0;
    M4_LSM_merge_M4_time_span_SEARCH_ARRAY_a_verifBPTP_cnt = 0;
    M4_LSM_merge_M4_time_span_SEARCH_ARRAY_b_genFP_cnt = 0;
    M4_LSM_merge_M4_time_span_SEARCH_ARRAY_b_genLP_cnt = 0;
    M4_LSM_merge_M4_time_span_SEARCH_ARRAY_c_genBPTP_cnt = 0;
    M4_LSM_FP_B_READ_MEM_CHUNK_cnt =
        0; // for counting the number of calling BCD by each step of M4-LSM
    M4_LSM_FP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA_cnt = 0;
    M4_LSM_FP_SEARCH_ARRAY_a_verifBPTP_cnt = 0;
    M4_LSM_FP_SEARCH_ARRAY_b_genFP_cnt = 0;
    M4_LSM_FP_SEARCH_ARRAY_b_genLP_cnt = 0;
    M4_LSM_FP_SEARCH_ARRAY_c_genBPTP_cnt = 0;
    M4_LSM_LP_B_READ_MEM_CHUNK_cnt =
        0; // for counting the number of calling BCD by each step of M4-LSM
    M4_LSM_LP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA_cnt = 0;
    M4_LSM_LP_SEARCH_ARRAY_a_verifBPTP_cnt = 0;
    M4_LSM_LP_SEARCH_ARRAY_b_genFP_cnt = 0;
    M4_LSM_LP_SEARCH_ARRAY_b_genLP_cnt = 0;
    M4_LSM_LP_SEARCH_ARRAY_c_genBPTP_cnt = 0;
    M4_LSM_BP_B_READ_MEM_CHUNK_cnt =
        0; // for counting the number of calling BCD by each step of M4-LSM
    M4_LSM_BP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA_cnt = 0;
    M4_LSM_BP_SEARCH_ARRAY_a_verifBPTP_cnt = 0;
    M4_LSM_BP_SEARCH_ARRAY_b_genFP_cnt = 0;
    M4_LSM_BP_SEARCH_ARRAY_b_genLP_cnt = 0;
    M4_LSM_BP_SEARCH_ARRAY_c_genBPTP_cnt = 0;
    M4_LSM_TP_B_READ_MEM_CHUNK_cnt =
        0; // for counting the number of calling BCD by each step of M4-LSM
    M4_LSM_TP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA_cnt = 0;
    M4_LSM_TP_SEARCH_ARRAY_a_verifBPTP_cnt = 0;
    M4_LSM_TP_SEARCH_ARRAY_b_genFP_cnt = 0;
    M4_LSM_TP_SEARCH_ARRAY_b_genLP_cnt = 0;
    M4_LSM_TP_SEARCH_ARRAY_c_genBPTP_cnt = 0;
  }

  public static class ValuePoint implements Comparable<ValuePoint> {
    public final int index;
    public final long value;

    public ValuePoint(int index, long value) {
      this.index = index;
      this.value = value;
    }

    @Override
    public int compareTo(ValuePoint other) {
      // multiplied to -1 as the author need descending sort order
      return -1 * Long.valueOf(this.value).compareTo(other.value);
    }
  }

  public static void addMeasure(Operation operation, long elapsedTimeInNanosecond) {
    switch (operation) {
      case DCP_Server_Query_Execute:
        DCP_Server_Query_Execute_count++;
        DCP_Server_Query_Execute_ns += elapsedTimeInNanosecond;
        break;
      case DCP_Server_Query_Fetch:
        DCP_Server_Query_Fetch_count++;
        DCP_Server_Query_Fetch_ns += elapsedTimeInNanosecond;
        break;
      case M4_LSM_INIT_LOAD_ALL_CHUNKMETADATAS:
        M4_LSM_init_loadAllChunkMetadatas_count++;
        M4_LSM_init_loadAllChunkMetadatas_ns += elapsedTimeInNanosecond;
        break;
      case M4_LSM_MERGE_M4_TIME_SPAN:
        M4_LSM_merge_M4_time_span_count++;
        M4_LSM_merge_M4_time_span_ns += elapsedTimeInNanosecond;
        break;
      case M4_LSM_FP:
        M4_LSM_FP_count++;
        M4_LSM_FP_ns += elapsedTimeInNanosecond;
        break;
      case M4_LSM_LP:
        M4_LSM_LP_count++;
        M4_LSM_LP_ns += elapsedTimeInNanosecond;
        break;
      case M4_LSM_BP:
        M4_LSM_BP_count++;
        M4_LSM_BP_ns += elapsedTimeInNanosecond;
        break;
      case M4_LSM_TP:
        M4_LSM_TP_count++;
        M4_LSM_TP_ns += elapsedTimeInNanosecond;
        break;
      case DCP_A_GET_CHUNK_METADATAS:
        DCP_A_GET_CHUNK_METADATAS_count++;
        DCP_A_GET_CHUNK_METADATAS_ns += elapsedTimeInNanosecond;
        break;
      case DCP_B_READ_MEM_CHUNK:
        DCP_B_READ_MEM_CHUNK_count++;
        DCP_B_READ_MEM_CHUNK_ns += elapsedTimeInNanosecond;
        if (M4_LSM_status != null) {
          switch (M4_LSM_status) {
            case M4_LSM_MERGE_M4_TIME_SPAN:
              M4_LSM_merge_M4_time_span_B_READ_MEM_CHUNK_cnt++;
              break;
            case M4_LSM_FP:
              M4_LSM_FP_B_READ_MEM_CHUNK_cnt++;
              break;
            case M4_LSM_LP:
              M4_LSM_LP_B_READ_MEM_CHUNK_cnt++;
              break;
            case M4_LSM_BP:
              M4_LSM_BP_B_READ_MEM_CHUNK_cnt++;
              break;
            case M4_LSM_TP:
              M4_LSM_TP_B_READ_MEM_CHUNK_cnt++;
              break;
            default:
              System.out.println("unsupported M4_LSM_status!");
              break;
          }
        }
        break;
      case DCP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA:
        DCP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA_count++;
        DCP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA_ns += elapsedTimeInNanosecond;
        if (M4_LSM_status != null) {
          // this actually can be omitted because the number is equal to B as chunk = page in this
          // case
          switch (M4_LSM_status) {
            case M4_LSM_MERGE_M4_TIME_SPAN:
              M4_LSM_merge_M4_time_span_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA_cnt++;
              break;
            case M4_LSM_FP:
              M4_LSM_FP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA_cnt++;
              break;
            case M4_LSM_LP:
              M4_LSM_LP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA_cnt++;
              break;
            case M4_LSM_BP:
              M4_LSM_BP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA_cnt++;
              break;
            case M4_LSM_TP:
              M4_LSM_TP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA_cnt++;
              break;
            default:
              System.out.println("unsupported M4_LSM_status!");
              break;
          }
        }
        break;
      case DCP_D_DECODE_PAGEDATA_TRAVERSE_POINTS:
        DCP_D_DECODE_PAGEDATA_TRAVERSE_POINTS_count++;
        DCP_D_DECODE_PAGEDATA_TRAVERSE_POINTS_ns += elapsedTimeInNanosecond;
        // M4-LSM will not use this op
        break;
      case SEARCH_ARRAY_a_verifBPTP:
        SEARCH_ARRAY_a_verifBPTP_count++;
        SEARCH_ARRAY_a_verifBPTP_ns += elapsedTimeInNanosecond;
        if (M4_LSM_status != null) {
          switch (M4_LSM_status) {
            case M4_LSM_MERGE_M4_TIME_SPAN:
              M4_LSM_merge_M4_time_span_SEARCH_ARRAY_a_verifBPTP_cnt++;
              break;
            case M4_LSM_FP:
              M4_LSM_FP_SEARCH_ARRAY_a_verifBPTP_cnt++;
              break;
            case M4_LSM_LP:
              M4_LSM_LP_SEARCH_ARRAY_a_verifBPTP_cnt++;
              break;
            case M4_LSM_BP:
              M4_LSM_BP_SEARCH_ARRAY_a_verifBPTP_cnt++;
              break;
            case M4_LSM_TP:
              M4_LSM_TP_SEARCH_ARRAY_a_verifBPTP_cnt++;
              break;
            default:
              System.out.println("unsupported M4_LSM_status!");
              break;
          }
        }
        break;
      case SEARCH_ARRAY_b_genFP:
        SEARCH_ARRAY_b_genFP_count++;
        SEARCH_ARRAY_b_genFP_ns += elapsedTimeInNanosecond;
        if (M4_LSM_status != null) {
          switch (M4_LSM_status) {
            case M4_LSM_MERGE_M4_TIME_SPAN:
              M4_LSM_merge_M4_time_span_SEARCH_ARRAY_b_genFP_cnt++;
              break;
            case M4_LSM_FP:
              M4_LSM_FP_SEARCH_ARRAY_b_genFP_cnt++;
              break;
            case M4_LSM_LP:
              M4_LSM_LP_SEARCH_ARRAY_b_genFP_cnt++;
              break;
            case M4_LSM_BP:
              M4_LSM_BP_SEARCH_ARRAY_b_genFP_cnt++;
              break;
            case M4_LSM_TP:
              M4_LSM_TP_SEARCH_ARRAY_b_genFP_cnt++;
              break;
            default:
              System.out.println("unsupported M4_LSM_status!");
              break;
          }
        }
        break;
      case SEARCH_ARRAY_b_genLP:
        SEARCH_ARRAY_b_genLP_count++;
        SEARCH_ARRAY_b_genLP_ns += elapsedTimeInNanosecond;
        if (M4_LSM_status != null) {
          switch (M4_LSM_status) {
            case M4_LSM_MERGE_M4_TIME_SPAN:
              M4_LSM_merge_M4_time_span_SEARCH_ARRAY_b_genLP_cnt++;
              break;
            case M4_LSM_FP:
              M4_LSM_FP_SEARCH_ARRAY_b_genLP_cnt++;
              break;
            case M4_LSM_LP:
              M4_LSM_LP_SEARCH_ARRAY_b_genLP_cnt++;
              break;
            case M4_LSM_BP:
              M4_LSM_BP_SEARCH_ARRAY_b_genLP_cnt++;
              break;
            case M4_LSM_TP:
              M4_LSM_TP_SEARCH_ARRAY_b_genLP_cnt++;
              break;
            default:
              System.out.println("unsupported M4_LSM_status!");
              break;
          }
        }
        break;
      case SEARCH_ARRAY_c_genBPTP:
        SEARCH_ARRAY_c_genBPTP_count++;
        SEARCH_ARRAY_c_genBPTP_ns += elapsedTimeInNanosecond;
        if (M4_LSM_status != null) {
          switch (M4_LSM_status) {
            case M4_LSM_MERGE_M4_TIME_SPAN:
              M4_LSM_merge_M4_time_span_SEARCH_ARRAY_c_genBPTP_cnt++;
              break;
            case M4_LSM_FP:
              M4_LSM_FP_SEARCH_ARRAY_c_genBPTP_cnt++;
              break;
            case M4_LSM_LP:
              M4_LSM_LP_SEARCH_ARRAY_c_genBPTP_cnt++;
              break;
            case M4_LSM_BP:
              M4_LSM_BP_SEARCH_ARRAY_c_genBPTP_cnt++;
              break;
            case M4_LSM_TP:
              M4_LSM_TP_SEARCH_ARRAY_c_genBPTP_cnt++;
              break;
            default:
              System.out.println("unsupported M4_LSM_status!");
              break;
          }
        }
        break;
      default:
        System.out.println("not supported operation type"); // this will not happen
        break;
    }
  }

  public static String print() {
    StringBuilder stringBuilder = new StringBuilder();
    // [1-cnt] is client elapsed time, not measured by the server side
    stringBuilder
        .append("[2-ns]Server_Query_Execute")
        .append(",")
        .append(DCP_Server_Query_Execute_ns)
        .append("\n");
    stringBuilder
        .append("[2-ns]Server_Query_Fetch")
        .append(",")
        .append(DCP_Server_Query_Fetch_ns)
        .append("\n");

    stringBuilder.append("[3]dataSetType").append(",").append(dataSetType).append("\n");
    stringBuilder
        .append("[3-ns]M4_LSM_init_loadAllChunkMetadatas")
        .append(",")
        .append(M4_LSM_init_loadAllChunkMetadatas_ns)
        .append("\n");
    stringBuilder
        .append("[3-ns]M4_LSM_merge_M4_time_span")
        .append(",")
        .append(M4_LSM_merge_M4_time_span_ns)
        .append("\n");
    stringBuilder.append("[3-ns]M4_LSM_FP").append(",").append(M4_LSM_FP_ns).append("\n");
    stringBuilder.append("[3-ns]M4_LSM_LP").append(",").append(M4_LSM_LP_ns).append("\n");
    stringBuilder.append("[3-ns]M4_LSM_BP").append(",").append(M4_LSM_BP_ns).append("\n");
    stringBuilder.append("[3-ns]M4_LSM_TP").append(",").append(M4_LSM_TP_ns).append("\n");

    stringBuilder
        .append("[4-ns]DCP_A_GET_CHUNK_METADATAS")
        .append(",")
        .append(DCP_A_GET_CHUNK_METADATAS_ns)
        .append("\n");
    stringBuilder
        .append("[4-ns]DCP_B_READ_MEM_CHUNK")
        .append(",")
        .append(DCP_B_READ_MEM_CHUNK_ns)
        .append("\n");
    stringBuilder
        .append("[4-ns]DCP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA")
        .append(",")
        .append(DCP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA_ns)
        .append("\n");
    stringBuilder
        .append("[4-ns]DCP_D_DECODE_PAGEDATA_TRAVERSE_POINTS")
        .append(",")
        .append(DCP_D_DECODE_PAGEDATA_TRAVERSE_POINTS_ns)
        .append("\n");
    stringBuilder
        .append("[4-ns]SEARCH_ARRAY_a_verifBPTP")
        .append(",")
        .append(SEARCH_ARRAY_a_verifBPTP_ns)
        .append("\n");
    stringBuilder
        .append("[4-ns]SEARCH_ARRAY_b_genFP")
        .append(",")
        .append(SEARCH_ARRAY_b_genFP_ns)
        .append("\n");
    stringBuilder
        .append("[4-ns]SEARCH_ARRAY_b_genLP")
        .append(",")
        .append(SEARCH_ARRAY_b_genLP_ns)
        .append("\n");
    stringBuilder
        .append("[4-ns]SEARCH_ARRAY_c_genBPTP")
        .append(",")
        .append(SEARCH_ARRAY_c_genBPTP_ns)
        .append("\n");

    stringBuilder
        .append("[2-cnt]Server_Query_Execute")
        .append(",")
        .append(DCP_Server_Query_Execute_count)
        .append("\n");
    stringBuilder
        .append("[2-cnt]Server_Query_Fetch")
        .append(",")
        .append(DCP_Server_Query_Fetch_count)
        .append("\n");

    stringBuilder
        .append("[3-cnt]M4_LSM_init_loadAllChunkMetadatas")
        .append(",")
        .append(M4_LSM_init_loadAllChunkMetadatas_count)
        .append("\n");
    stringBuilder
        .append("[3-cnt]M4_LSM_merge_M4_time_span")
        .append(",")
        .append(M4_LSM_merge_M4_time_span_count)
        .append("\n");
    stringBuilder.append("[3-cnt]M4_LSM_FP").append(",").append(M4_LSM_FP_count).append("\n");
    stringBuilder.append("[3-cnt]M4_LSM_LP").append(",").append(M4_LSM_LP_count).append("\n");
    stringBuilder.append("[3-cnt]M4_LSM_BP").append(",").append(M4_LSM_BP_count).append("\n");
    stringBuilder.append("[3-cnt]M4_LSM_TP").append(",").append(M4_LSM_TP_count).append("\n");

    stringBuilder
        .append("[4-cnt]DCP_A_GET_CHUNK_METADATAS")
        .append(",")
        .append(DCP_A_GET_CHUNK_METADATAS_count)
        .append("\n");
    stringBuilder
        .append("[4-cnt]DCP_B_READ_MEM_CHUNK")
        .append(",")
        .append(DCP_B_READ_MEM_CHUNK_count)
        .append("\n");
    stringBuilder
        .append("[4-cnt]DCP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA")
        .append(",")
        .append(DCP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA_count)
        .append("\n");
    stringBuilder
        .append("[4-cnt]DCP_D_DECODE_PAGEDATA_TRAVERSE_POINTS")
        .append(",")
        .append(DCP_D_DECODE_PAGEDATA_TRAVERSE_POINTS_count)
        .append("\n");
    stringBuilder
        .append("[4-cnt]SEARCH_ARRAY_a_verifBPTP")
        .append(",")
        .append(SEARCH_ARRAY_a_verifBPTP_count)
        .append("\n");
    stringBuilder
        .append("[4-cnt]SEARCH_ARRAY_b_genFP")
        .append(",")
        .append(SEARCH_ARRAY_b_genFP_count)
        .append("\n");
    stringBuilder
        .append("[4-cnt]SEARCH_ARRAY_b_genLP")
        .append(",")
        .append(SEARCH_ARRAY_b_genLP_count)
        .append("\n");
    stringBuilder
        .append("[4-cnt]SEARCH_ARRAY_c_genBPTP")
        .append(",")
        .append(SEARCH_ARRAY_c_genBPTP_count)
        .append("\n");
    stringBuilder
        .append("[4-cnt]DCP_D_traversedPointNum")
        .append(",")
        .append(DCP_D_traversedPointNum)
        .append("\n");

    stringBuilder
        .append("[3-4]M4_LSM_merge_M4_time_span_B_READ_MEM_CHUNK_cnt")
        .append(",")
        .append(M4_LSM_merge_M4_time_span_B_READ_MEM_CHUNK_cnt)
        .append("\n"); // 5*6
    stringBuilder
        .append("[3-4]M4_LSM_merge_M4_time_span_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA_cnt")
        .append(",")
        .append(M4_LSM_merge_M4_time_span_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA_cnt)
        .append("\n");
    stringBuilder
        .append("[3-4]M4_LSM_merge_M4_time_span_SEARCH_ARRAY_a_verifBPTP_cnt")
        .append(",")
        .append(M4_LSM_merge_M4_time_span_SEARCH_ARRAY_a_verifBPTP_cnt)
        .append("\n");
    stringBuilder
        .append("[3-4]M4_LSM_merge_M4_time_span_SEARCH_ARRAY_b_genFP_cnt")
        .append(",")
        .append(M4_LSM_merge_M4_time_span_SEARCH_ARRAY_b_genFP_cnt)
        .append("\n");
    stringBuilder
        .append("[3-4]M4_LSM_merge_M4_time_span_SEARCH_ARRAY_b_genLP_cnt")
        .append(",")
        .append(M4_LSM_merge_M4_time_span_SEARCH_ARRAY_b_genLP_cnt)
        .append("\n");
    stringBuilder
        .append("[3-4]M4_LSM_merge_M4_time_span_SEARCH_ARRAY_c_genBPTP_cnt")
        .append(",")
        .append(M4_LSM_merge_M4_time_span_SEARCH_ARRAY_c_genBPTP_cnt)
        .append("\n");

    stringBuilder
        .append("[3-4]M4_LSM_FP_B_READ_MEM_CHUNK_cnt")
        .append(",")
        .append(M4_LSM_FP_B_READ_MEM_CHUNK_cnt)
        .append("\n"); // 5*6
    stringBuilder
        .append("[3-4]M4_LSM_FP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA_cnt")
        .append(",")
        .append(M4_LSM_FP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA_cnt)
        .append("\n");
    stringBuilder
        .append("[3-4]M4_LSM_FP_SEARCH_ARRAY_a_verifBPTP_cnt")
        .append(",")
        .append(M4_LSM_FP_SEARCH_ARRAY_a_verifBPTP_cnt)
        .append("\n");
    stringBuilder
        .append("[3-4]M4_LSM_FP_SEARCH_ARRAY_b_genFP_cnt")
        .append(",")
        .append(M4_LSM_FP_SEARCH_ARRAY_b_genFP_cnt)
        .append("\n");
    stringBuilder
        .append("[3-4]M4_LSM_FP_SEARCH_ARRAY_b_genLP_cnt")
        .append(",")
        .append(M4_LSM_FP_SEARCH_ARRAY_b_genLP_cnt)
        .append("\n");
    stringBuilder
        .append("[3-4]M4_LSM_FP_SEARCH_ARRAY_c_genBPTP_cnt")
        .append(",")
        .append(M4_LSM_FP_SEARCH_ARRAY_c_genBPTP_cnt)
        .append("\n");

    stringBuilder
        .append("[3-4]M4_LSM_LP_B_READ_MEM_CHUNK_cnt")
        .append(",")
        .append(M4_LSM_LP_B_READ_MEM_CHUNK_cnt)
        .append("\n"); // 5*6
    stringBuilder
        .append("[3-4]M4_LSM_LP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA_cnt")
        .append(",")
        .append(M4_LSM_LP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA_cnt)
        .append("\n");
    stringBuilder
        .append("[3-4]M4_LSM_LP_SEARCH_ARRAY_a_verifBPTP_cnt")
        .append(",")
        .append(M4_LSM_LP_SEARCH_ARRAY_a_verifBPTP_cnt)
        .append("\n");
    stringBuilder
        .append("[3-4]M4_LSM_LP_SEARCH_ARRAY_b_genFP_cnt")
        .append(",")
        .append(M4_LSM_LP_SEARCH_ARRAY_b_genFP_cnt)
        .append("\n");
    stringBuilder
        .append("[3-4]M4_LSM_LP_SEARCH_ARRAY_b_genLP_cnt")
        .append(",")
        .append(M4_LSM_LP_SEARCH_ARRAY_b_genLP_cnt)
        .append("\n");
    stringBuilder
        .append("[3-4]M4_LSM_LP_SEARCH_ARRAY_c_genBPTP_cnt")
        .append(",")
        .append(M4_LSM_LP_SEARCH_ARRAY_c_genBPTP_cnt)
        .append("\n");

    stringBuilder
        .append("[3-4]M4_LSM_BP_B_READ_MEM_CHUNK_cnt")
        .append(",")
        .append(M4_LSM_BP_B_READ_MEM_CHUNK_cnt)
        .append("\n"); // 5*6
    stringBuilder
        .append("[3-4]M4_LSM_BP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA_cnt")
        .append(",")
        .append(M4_LSM_BP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA_cnt)
        .append("\n");
    stringBuilder
        .append("[3-4]M4_LSM_BP_SEARCH_ARRAY_a_verifBPTP_cnt")
        .append(",")
        .append(M4_LSM_BP_SEARCH_ARRAY_a_verifBPTP_cnt)
        .append("\n");
    stringBuilder
        .append("[3-4]M4_LSM_BP_SEARCH_ARRAY_b_genFP_cnt")
        .append(",")
        .append(M4_LSM_BP_SEARCH_ARRAY_b_genFP_cnt)
        .append("\n");
    stringBuilder
        .append("[3-4]M4_LSM_BP_SEARCH_ARRAY_b_genLP_cnt")
        .append(",")
        .append(M4_LSM_BP_SEARCH_ARRAY_b_genLP_cnt)
        .append("\n");
    stringBuilder
        .append("[3-4]M4_LSM_BP_SEARCH_ARRAY_c_genBPTP_cnt")
        .append(",")
        .append(M4_LSM_BP_SEARCH_ARRAY_c_genBPTP_cnt)
        .append("\n");

    stringBuilder
        .append("[3-4]M4_LSM_TP_B_READ_MEM_CHUNK_cnt")
        .append(",")
        .append(M4_LSM_TP_B_READ_MEM_CHUNK_cnt)
        .append("\n"); // 5*6
    stringBuilder
        .append("[3-4]M4_LSM_TP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA_cnt")
        .append(",")
        .append(M4_LSM_TP_C_DESERIALIZE_PAGEHEADER_DECOMPRESS_PAGEDATA_cnt)
        .append("\n");
    stringBuilder
        .append("[3-4]M4_LSM_TP_SEARCH_ARRAY_a_verifBPTP_cnt")
        .append(",")
        .append(M4_LSM_TP_SEARCH_ARRAY_a_verifBPTP_cnt)
        .append("\n");
    stringBuilder
        .append("[3-4]M4_LSM_TP_SEARCH_ARRAY_b_genFP_cnt")
        .append(",")
        .append(M4_LSM_TP_SEARCH_ARRAY_b_genFP_cnt)
        .append("\n");
    stringBuilder
        .append("[3-4]M4_LSM_TP_SEARCH_ARRAY_b_genLP_cnt")
        .append(",")
        .append(M4_LSM_TP_SEARCH_ARRAY_b_genLP_cnt)
        .append("\n");
    stringBuilder
        .append("[3-4]M4_LSM_TP_SEARCH_ARRAY_c_genBPTP_cnt")
        .append(",")
        .append(M4_LSM_TP_SEARCH_ARRAY_c_genBPTP_cnt)
        .append("\n");

    reset(); // whenever print() is called, reset the metrics, to clean warm up information.
    return stringBuilder.toString();
  }
}
