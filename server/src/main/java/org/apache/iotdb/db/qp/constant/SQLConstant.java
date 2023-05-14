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
package org.apache.iotdb.db.qp.constant;

import org.apache.iotdb.db.metadata.path.PartialPath;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** this class contains several constants used in SQL. */
@SuppressWarnings("unused") // some fields are for future features
public class SQLConstant {

  private SQLConstant() {
    // forbidding instantiation
  }

  private static final String[] SINGLE_ROOT_ARRAY = {"root", "**"};
  private static final String[] SINGLE_TIME_ARRAY = {"time"};
  public static final PartialPath TIME_PATH = new PartialPath(SINGLE_TIME_ARRAY);
  public static final String ALIGNBY_DEVICE_COLUMN_NAME = "Device";
  public static final String RESERVED_TIME = "time";
  public static final String IS_AGGREGATION = "IS_AGGREGATION";
  public static final String NOW_FUNC = "now()";
  public static final String START_TIME_STR = "1970-1-01T00:00:00";

  public static final String LINE_FEED_SIGNAL = "\n";
  public static final String ROOT = "root";
  public static final String METADATA_PARAM_EQUAL = "=";
  public static final String QUOTE = "'";
  public static final String DQUOTE = "\"";
  public static final String BOOLEAN_TRUE = "true";
  public static final String BOOLEAN_FALSE = "false";
  public static final String BOOLEAN_TRUE_NUM = "1";
  public static final String BOOLEAN_FALSE_NUM = "0";

  // names of aggregations
  public static final String MIN_TIME = "min_time";
  public static final String MAX_TIME = "max_time";

  public static final String MAX_VALUE = "max_value";
  public static final String MIN_VALUE = "min_value";

  public static final String EXTREME = "extreme";

  public static final String FIRST_VALUE = "first_value";
  public static final String LAST_VALUE = "last_value";

  public static final String LAST = "last";

  public static final String COUNT = "count";
  public static final String AVG = "avg";
  public static final String SUM = "sum";
  public static final String EXACT_MEDIAN = "exact_median";
  public static final String EXACT_MEDIAN_OPT = "exact_median_opt";
  public static final String EXACT_MEDIAN_OPT_2 = "exact_median_opt_2";
  public static final String EXACT_MEDIAN_OPT_3 = "exact_median_opt_3";
  public static final String EXACT_MEDIAN_OPT_4 = "exact_median_opt_4";
  public static final String EXACT_MEDIAN_OPT_5 = "exact_median_opt_5";
  public static final String EXACT_MEDIAN_AMORTIZED = "exact_median_amortized";
  public static final String EXACT_MEDIAN_KLL_FLOATS = "exact_median_kll_floats";
  public static final String EXACT_MEDIAN_AGGRESSIVE = "exact_median_aggressive";
  public static final String EXACT_MEDIAN_BITS_BUCKET_STAT = "exact_median_bits_bucket_stat";
  public static final String EXACT_MEDIAN_BITS_BUCKET_STAT_FILTER =
      "exact_median_bits_bucket_stat_filter";
  public static final String EXACT_MEDIAN_BITS_BUCKET_STAT_FILTER_AGGRESSIVE =
      "exact_median_bits_bucket_stat_filter_aggressive";
  public static final String EXACT_MEDIAN_KLL_STAT = "exact_median_kll_stat";
  public static final String EXACT_MEDIAN_KLL_STAT_SINGLE = "exact_median_kll_stat_single";
  public static final String EXACT_MEDIAN_KLL_FLOATS_SINGLE = "exact_median_kll_floats_single";
  public static final String EXACT_MEDIAN_KLL_STAT_SINGLE_READ =
      "exact_median_kll_stat_single_read";
  public static final String EXACT_MEDIAN_KLL_DEBUG = "exact_median_kll_debug";
  public static final String EXACT_MEDIAN_KLL_STAT_DEBUG_FULL_READING =
      "exact_median_kll_stat_debug_full_reading";
  public static final String EXACT_MEDIAN_KLL_DEBUG_FULL_READING =
      "exact_median_kll_debug_full_reading";
  public static final String EXACT_MEDIAN_KLL_STAT_DEBUG_PAGE_DEMAND_RATE =
      "exact_median_kll_stat_debug_page_demand_rate";
  public static final String EXACT_MEDIAN_KLL_STAT_OVERLAP_SINGLE =
      "exact_median_kll_stat_overlap_single";
  public static final String TDIGEST_STAT_SINGLE = "tdigest_quantile";
  public static final String SAMPLING_STAT_SINGLE = "sampling_quantile";
  public static final String STRICT_KLL_STAT_SINGLE = "kll_quantile";
  public static final String DDSKETCH_SINGLE = "ddsketch_quantile";
  public static final String CHUNK_STAT_AVAIL = "chunk_stat_available";
  public static final String EXACT_QUANTILE_BASELINE_KLL = "exact_quantile_baseline_kll";
  public static final String EXACT_QUANTILE_PR_KLL_NO_OPT = "exact_quantile_pr_kll_no_opt";
  public static final String EXACT_QUANTILE_DDSKETCH = "exact_quantile_ddsketch";
  public static final String EXACT_QUANTILE_PR_KLL_OPT_STAT = "exact_quantile_pr_kll_opt_stat";
  public static final String EXACT_QUANTILE_PR_KLL_OPT_FILTER = "exact_quantile_pr_kll_opt_filter";
  public static final String EXACT_QUANTILE_PR_KLL_OPT_SUMMARY =
      "exact_quantile_pr_kll_opt_summary";
  public static final String FULL_READ_ONCE = "full_read_once";
  public static final String EXACT_QUANTILE_QUICK_SELECT = "exact_quantile_quick_select";
  public static final String EXACT_MULTI_QUANTILES_QUICK_SELECT =
      "exact_multi_quantiles_quick_select";
  public static final String EXACT_MULTI_QUANTILES_PR_KLL_OPT_SUMMARY =
      "exact_multi_quantiles_pr_kll_opt_summary";
  public static final String EXACT_QUANTILE_PR_KLL_PRIORI_FIX_PR =
      "exact_quantile_pr_kll_priori_fix_pr";
  public static final String EXACT_QUANTILE_PR_KLL_PRIORI_BEST_PR =
      "exact_quantile_pr_kll_priori_best_pr";
  public static final String EXACT_QUANTILE_PR_KLL_POST_BEST_PR =
      "exact_quantile_pr_kll_post_best_pr";
  public static final String EXACT_QUANTILE_MRL = "exact_quantile_mrl";
  public static final String EXACT_QUANTILE_TDIGEST = "exact_quantile_tdigest";
  public static final String EXACT_QUANTILE_DDSKETCH_POSITIVE = "exact_quantile_ddsketch_positive";
  public static final String EXACT_MULTI_QUANTILES_PR_KLL_POST_BEST_PR =
      "exact_multi_quantiles_pr_kll_post_best_pr";
  public static final String EXACT_MULTI_QUANTILES_PR_KLL_FIX_PR =
      "exact_multi_quantiles_pr_kll_fix_pr";
  public static final String EXACT_MULTI_QUANTILES_MRL = "exact_multi_quantiles_mrl";
  public static final String EXACT_MULTI_QUANTILES_TDIGEST = "exact_multi_quantiles_tdigest";
  public static final String EXACT_MULTI_QUANTILES_DDSKETCH_POSITIVE =
      "exact_multi_quantiles_ddsketch_positive";
  public static final String MAD_DD = "mad_dd";
  public static final String MAD_QS = "mad_qs";
  public static final String MAD_CORE = "mad_core";

  public static final String ALL = "all";

  private static final Set<String> NATIVE_FUNCTION_NAMES =
      new HashSet<>(
          Arrays.asList(
              MIN_TIME,
              MAX_TIME,
              MIN_VALUE,
              MAX_VALUE,
              EXTREME,
              FIRST_VALUE,
              LAST_VALUE,
              COUNT,
              SUM,
              AVG,
              EXACT_MEDIAN,
              EXACT_MEDIAN_OPT,
              EXACT_MEDIAN_OPT_2,
              EXACT_MEDIAN_OPT_3,
              EXACT_MEDIAN_OPT_4,
              EXACT_MEDIAN_OPT_5,
              EXACT_MEDIAN_AMORTIZED,
              EXACT_MEDIAN_KLL_FLOATS,
              EXACT_MEDIAN_AGGRESSIVE,
              EXACT_MEDIAN_BITS_BUCKET_STAT,
              EXACT_MEDIAN_BITS_BUCKET_STAT_FILTER,
              EXACT_MEDIAN_BITS_BUCKET_STAT_FILTER_AGGRESSIVE,
              EXACT_MEDIAN_KLL_STAT,
              EXACT_MEDIAN_KLL_STAT_SINGLE,
              EXACT_MEDIAN_KLL_FLOATS_SINGLE,
              EXACT_MEDIAN_KLL_STAT_SINGLE_READ,
              EXACT_MEDIAN_KLL_DEBUG,
              EXACT_MEDIAN_KLL_STAT_DEBUG_FULL_READING,
              EXACT_MEDIAN_KLL_DEBUG_FULL_READING,
              EXACT_MEDIAN_KLL_STAT_DEBUG_PAGE_DEMAND_RATE,
              EXACT_MEDIAN_KLL_STAT_OVERLAP_SINGLE,
              TDIGEST_STAT_SINGLE,
              SAMPLING_STAT_SINGLE,
              STRICT_KLL_STAT_SINGLE,
              DDSKETCH_SINGLE,
              CHUNK_STAT_AVAIL,
              EXACT_QUANTILE_BASELINE_KLL,
              EXACT_QUANTILE_PR_KLL_NO_OPT,
              EXACT_QUANTILE_DDSKETCH,
              EXACT_QUANTILE_PR_KLL_OPT_STAT,
              EXACT_QUANTILE_PR_KLL_OPT_FILTER,
              EXACT_QUANTILE_PR_KLL_OPT_SUMMARY,
              FULL_READ_ONCE,
              EXACT_QUANTILE_QUICK_SELECT,
              EXACT_MULTI_QUANTILES_QUICK_SELECT,
              EXACT_MULTI_QUANTILES_PR_KLL_OPT_SUMMARY,
              EXACT_QUANTILE_PR_KLL_PRIORI_FIX_PR,
              EXACT_QUANTILE_PR_KLL_PRIORI_BEST_PR,
              EXACT_QUANTILE_PR_KLL_POST_BEST_PR,
              EXACT_QUANTILE_MRL,
              EXACT_QUANTILE_TDIGEST,
              EXACT_QUANTILE_DDSKETCH_POSITIVE,
              EXACT_MULTI_QUANTILES_PR_KLL_POST_BEST_PR,
              EXACT_MULTI_QUANTILES_PR_KLL_FIX_PR,
              EXACT_MULTI_QUANTILES_MRL,
              EXACT_MULTI_QUANTILES_TDIGEST,
              EXACT_MULTI_QUANTILES_DDSKETCH_POSITIVE,
              MAD_DD,
              MAD_QS,
              MAD_CORE));

  public static final int TOK_WHERE = 23;
  public static final int TOK_INSERT = 24;
  public static final int TOK_DELETE = 25;
  public static final int TOK_UPDATE = 26;
  public static final int TOK_QUERY = 27;

  public static final int TOK_CREATE_INDEX = 31;
  public static final int TOK_DROP_INDEX = 32;
  public static final int TOK_QUERY_INDEX = 33;

  public static final int TOK_GRANT_WATERMARK_EMBEDDING = 34;
  public static final int TOK_REVOKE_WATERMARK_EMBEDDING = 35;

  public static final int TOK_AUTHOR_CREATE = 41;
  public static final int TOK_AUTHOR_DROP = 42;
  public static final int TOK_AUTHOR_GRANT = 43;
  public static final int TOK_AUTHOR_REVOKE = 44;
  public static final int TOK_AUTHOR_UPDATE_USER = 46;

  public static final int TOK_DATALOAD = 45;

  public static final int TOK_METADATA_CREATE = 51;
  public static final int TOK_METADATA_DELETE = 52;
  public static final int TOK_METADATA_SET_FILE_LEVEL = 53;
  public static final int TOK_PROPERTY_CREATE = 54;
  public static final int TOK_PROPERTY_ADD_LABEL = 55;
  public static final int TOK_PROPERTY_DELETE_LABEL = 56;
  public static final int TOK_PROPERTY_LINK = 57;
  public static final int TOK_PROPERTY_UNLINK = 58;
  public static final int TOK_LIST = 59;

  public static final int TOK_DURATION = 60;
  public static final int TOK_DATE_EXPR = 61;
  public static final int TOK_METADATA_DELETE_FILE_LEVEL = 62;

  public static final int TOK_SET = 63;
  public static final int TOK_UNSET = 64;
  public static final int TOK_SHOW = 65;
  public static final int TOK_LOAD_CONFIGURATION = 66;

  public static final int TOK_FLUSH_TASK_INFO = 67;
  public static final int TOK_LOAD_FILES = 69;
  public static final int TOK_REMOVE_FILE = 70;
  public static final int TOK_UNLOAD_FILE = 71;
  public static final int TOK_VERSION = 72;
  public static final int TOK_TIMESERIES = 73;
  public static final int TOK_STORAGE_GROUP = 74;
  public static final int TOK_CHILD_PATHS = 75;
  public static final int TOK_DEVICES = 76;
  public static final int TOK_COUNT_TIMESERIES = 77;
  public static final int TOK_COUNT_NODE_TIMESERIES = 78;
  public static final int TOK_COUNT_NODES = 79;

  public static final int TOK_METADATA_ALTER = 80;

  public static final int TOK_FLUSH = 81;
  public static final int TOK_MERGE = 82;
  public static final int TOK_FULL_MERGE = 83;

  public static final int TOK_CLEAR_CACHE = 84;

  public static final int TOK_LOAD_CONFIGURATION_GLOBAL = 85;
  public static final int TOK_LOAD_CONFIGURATION_LOCAL = 86;

  public static final int TOK_SHOW_MERGE_STATUS = 87;
  public static final int TOK_DELETE_PARTITION = 88;

  public static final int TOK_CREATE_SCHEMA_SNAPSHOT = 89;
  public static final int TOK_TRACING = 91;

  public static final int TOK_FUNCTION_CREATE = 92;
  public static final int TOK_FUNCTION_DROP = 93;
  public static final int TOK_SHOW_FUNCTIONS = 94;

  public static final int TOK_COUNT_DEVICES = 95;
  public static final int TOK_COUNT_STORAGE_GROUP = 96;
  public static final int TOK_QUERY_PROCESSLIST = 97;
  public static final int TOK_KILL_QUERY = 98;

  public static final int TOK_CHILD_NODES = 99;

  public static final int TOK_TRIGGER_CREATE = 100;
  public static final int TOK_TRIGGER_DROP = 101;
  public static final int TOK_TRIGGER_START = 102;
  public static final int TOK_TRIGGER_STOP = 103;
  public static final int TOK_SHOW_TRIGGERS = 104;
  public static final int TOK_LOCK_INFO = 105;

  public static final int TOK_CONTINUOUS_QUERY_CREATE = 106;
  public static final int TOK_CONTINUOUS_QUERY_DROP = 107;
  public static final int TOK_SHOW_CONTINUOUS_QUERIES = 108;

  public static final int TOK_SELECT_INTO = 109;

  public static final int TOK_SET_SYSTEM_MODE = 110;

  public static final int TOK_SETTLE = 111;

  public static final int TOK_SCHEMA_TEMPLATE_CREATE = 112;
  public static final int TOK_SCHEMA_TEMPLATE_SET = 113;
  public static final int TOK_SCHEMA_TEMPLATE_ACTIVATE = 114;
  public static final int TOK_SCHEMA_TEMPLATE_UNSET = 115;
  public static final int TOK_SCHEMA_TEMPLATE_APPEND = 116;
  public static final int TOK_SCHEMA_TEMPLATE_PRUNE = 117;
  public static final int TOK_SCHEMA_TEMPLATE_DROP = 118;
  public static final int TOK_SCHEMA_TEMPLATE_SHOW = 119;
  public static final int TOK_SCHEMA_TEMPLATE_SHOW_NODES = 120;
  public static final int TOK_SCHEMA_TEMPLATE_SHOW_PATHS_SET = 121;
  public static final int TOK_SCHEMA_TEMPLATE_SHOW_PATHS_USING = 122;

  public static final int TOK_SHOW_QUERY_RESOURCE = 123;

  public static final Map<Integer, String> tokenNames = new HashMap<>();

  public static String[] getSingleRootArray() {
    return SINGLE_ROOT_ARRAY;
  }

  public static String[] getSingleTimeArray() {
    return SINGLE_TIME_ARRAY;
  }

  static {
    tokenNames.put(TOK_WHERE, "TOK_WHERE");
    tokenNames.put(TOK_INSERT, "TOK_INSERT");
    tokenNames.put(TOK_DELETE, "TOK_DELETE");
    tokenNames.put(TOK_UPDATE, "TOK_UPDATE");
    tokenNames.put(TOK_QUERY, "TOK_QUERY");

    tokenNames.put(TOK_AUTHOR_CREATE, "TOK_AUTHOR_CREATE");
    tokenNames.put(TOK_AUTHOR_DROP, "TOK_AUTHOR_DROP");
    tokenNames.put(TOK_AUTHOR_GRANT, "TOK_AUTHOR_GRANT");
    tokenNames.put(TOK_AUTHOR_REVOKE, "TOK_AUTHOR_REVOKE");
    tokenNames.put(TOK_AUTHOR_UPDATE_USER, "TOK_AUTHOR_UPDATE_USER");
    tokenNames.put(TOK_DATALOAD, "TOK_DATALOAD");

    tokenNames.put(TOK_METADATA_CREATE, "TOK_METADATA_CREATE");
    tokenNames.put(TOK_METADATA_DELETE, "TOK_METADATA_DELETE");
    tokenNames.put(TOK_METADATA_SET_FILE_LEVEL, "TOK_METADATA_SET_FILE_LEVEL");
    tokenNames.put(TOK_METADATA_DELETE_FILE_LEVEL, "TOK_METADATA_DELETE_FILE_LEVEL");
    tokenNames.put(TOK_PROPERTY_CREATE, "TOK_PROPERTY_CREATE");
    tokenNames.put(TOK_PROPERTY_ADD_LABEL, "TOK_PROPERTY_ADD_LABEL");
    tokenNames.put(TOK_PROPERTY_DELETE_LABEL, "TOK_PROPERTY_DELETE_LABEL");
    tokenNames.put(TOK_PROPERTY_LINK, "TOK_PROPERTY_LINK");
    tokenNames.put(TOK_PROPERTY_UNLINK, "TOK_PROPERTY_UNLINK");

    tokenNames.put(TOK_LIST, "TOK_LIST");
    tokenNames.put(TOK_SET, "TOK_SET");
    tokenNames.put(TOK_UNSET, "TOK_UNSET");
    tokenNames.put(TOK_SHOW, "TOK_SHOW");

    tokenNames.put(TOK_LOAD_CONFIGURATION, "TOK_LOAD_CONFIGURATION");
    tokenNames.put(TOK_FLUSH_TASK_INFO, "TOK_FLUSH_TASK_INFO");

    tokenNames.put(TOK_LOAD_FILES, "TOK_LOAD_FILES");
    tokenNames.put(TOK_REMOVE_FILE, "TOK_REMOVE_FILE");
    tokenNames.put(TOK_UNLOAD_FILE, "TOK_UNLOAD_FILE");

    tokenNames.put(TOK_SHOW_MERGE_STATUS, "TOK_SHOW_MERGE_STATUS");
    tokenNames.put(TOK_DELETE_PARTITION, "TOK_DELETE_PARTITION");

    tokenNames.put(TOK_TRACING, "TOK_TRACING");

    tokenNames.put(TOK_FUNCTION_CREATE, "TOK_FUNCTION_CREATE");
    tokenNames.put(TOK_FUNCTION_DROP, "TOK_FUNCTION_DROP");
    tokenNames.put(TOK_SHOW_FUNCTIONS, "TOK_SHOW_FUNCTIONS");

    tokenNames.put(TOK_CREATE_INDEX, "TOK_CREATE_INDEX");
    tokenNames.put(TOK_DROP_INDEX, "TOK_DROP_INDEX");
    tokenNames.put(TOK_QUERY_INDEX, "TOK_QUERY_INDEX");

    tokenNames.put(TOK_TRIGGER_CREATE, "TOK_TRIGGER_CREATE");
    tokenNames.put(TOK_TRIGGER_DROP, "TOK_TRIGGER_DROP");
    tokenNames.put(TOK_TRIGGER_START, "TOK_TRIGGER_START");
    tokenNames.put(TOK_TRIGGER_STOP, "TOK_TRIGGER_STOP");
    tokenNames.put(TOK_SHOW_TRIGGERS, "TOK_SHOW_TRIGGERS");

    tokenNames.put(TOK_CONTINUOUS_QUERY_CREATE, "TOK_CONTINUOUS_QUERY_CREATE");
    tokenNames.put(TOK_CONTINUOUS_QUERY_DROP, "TOK_CONTINUOUS_QUERY_DROP");
    tokenNames.put(TOK_SHOW_CONTINUOUS_QUERIES, "TOK_SHOW_CONTINUOUS_QUERIES");

    tokenNames.put(TOK_SELECT_INTO, "TOK_SELECT_INTO");

    tokenNames.put(TOK_SETTLE, "TOK_SETTLE");

    tokenNames.put(TOK_SCHEMA_TEMPLATE_CREATE, "TOK_SCHEMA_TEMPLATE_CREATE");
    tokenNames.put(TOK_SCHEMA_TEMPLATE_SET, "TOK_SCHEMA_TEMPLATE_SET");
    tokenNames.put(TOK_SCHEMA_TEMPLATE_ACTIVATE, "TOK_SCHEMA_TEMPLATE_ACTIVATE");
    tokenNames.put(TOK_SCHEMA_TEMPLATE_UNSET, "TOK_SCHEMA_TEMPLATE_UNSET");
    tokenNames.put(TOK_SCHEMA_TEMPLATE_APPEND, "TOK_SCHEMA_TEMPLATE_APPEND");
    tokenNames.put(TOK_SCHEMA_TEMPLATE_PRUNE, "TOK_SCHEMA_TEMPLATE_PRUNE");
    tokenNames.put(TOK_SCHEMA_TEMPLATE_DROP, "TOK_SCHEMA_TEMPLATE_DROP");
    tokenNames.put(TOK_SCHEMA_TEMPLATE_SHOW, "TOK_SCHEMA_TEMPLATE_SHOW");
    tokenNames.put(TOK_SCHEMA_TEMPLATE_SHOW_NODES, "TOK_SCHEMA_TEMPLATE_SHOW_NODES");
    tokenNames.put(TOK_SCHEMA_TEMPLATE_SHOW_PATHS_SET, "TOK_SCHEMA_TEMPLATE_SHOW_PATHS_SET");
    tokenNames.put(TOK_SCHEMA_TEMPLATE_SHOW_PATHS_USING, "TOK_SCHEMA_TEMPLATE_SHOW_PATHS_USING");

    tokenNames.put(TOK_SHOW_QUERY_RESOURCE, "TOK_SHOW_QUERY_RESOURCE");
  }

  public static boolean isReservedPath(PartialPath pathStr) {
    return pathStr.equals(TIME_PATH);
  }

  public static Set<String> getNativeFunctionNames() {
    return NATIVE_FUNCTION_NAMES;
  }
}
