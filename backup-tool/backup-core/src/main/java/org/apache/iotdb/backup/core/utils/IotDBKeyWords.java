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
package org.apache.iotdb.backup.core.utils;

import java.util.HashSet;
import java.util.Set;

/** @Author: LL @Description: @Date: create in 2022/7/6 14:55 */
public class IotDBKeyWords {

  public static final Set<String> KEYWORDS = new HashSet<>();

  static {
    KEYWORDS.add("ADD");
    KEYWORDS.add("AFTER");
    KEYWORDS.add("ALIAS");
    KEYWORDS.add("ALIGN");
    KEYWORDS.add("ALIGNED");
    KEYWORDS.add("ALL");
    KEYWORDS.add("ALTER");
    KEYWORDS.add("ANY");
    KEYWORDS.add("AS");
    KEYWORDS.add("ASC");
    KEYWORDS.add("ATTRIBUTES");
    KEYWORDS.add("AUTOREGISTER");
    KEYWORDS.add("BEFORE");
    KEYWORDS.add("BEGIN");
    KEYWORDS.add("BY");
    KEYWORDS.add("CACHE");
    KEYWORDS.add("CHILD");
    KEYWORDS.add("CLEAR");
    KEYWORDS.add("COMPRESSION");
    KEYWORDS.add("COMPRESSOR");
    KEYWORDS.add("CONCAT");
    KEYWORDS.add("CONFIGURATION");
    KEYWORDS.add("CONTINUOUS");
    KEYWORDS.add("COUNT");
    KEYWORDS.add("CONTAIN");
    KEYWORDS.add("CQ");
    KEYWORDS.add("CQS");
    KEYWORDS.add("CREATE");
    KEYWORDS.add("DATATYPE");
    KEYWORDS.add("DEBUG");
    KEYWORDS.add("DELETE");
    KEYWORDS.add("DESC");
    KEYWORDS.add("DESCRIBE");
    KEYWORDS.add("DEVICE");
    KEYWORDS.add("DEVICES");
    KEYWORDS.add("DISABLE");
    KEYWORDS.add("DROP");
    KEYWORDS.add("ENCODING");
    KEYWORDS.add("END");
    KEYWORDS.add("EVERY");
    KEYWORDS.add("EXPLAIN");
    KEYWORDS.add("FILL");
    KEYWORDS.add("FLUSH");
    KEYWORDS.add("FOR");
    KEYWORDS.add("FROM");
    KEYWORDS.add("FULL");
    KEYWORDS.add("FUNCTION");
    KEYWORDS.add("FUNCTIONS");
    KEYWORDS.add("GLOBAL");
    KEYWORDS.add("GRANT");
    KEYWORDS.add("GROUP");
    KEYWORDS.add("INDEX");
    KEYWORDS.add("INFO");
    KEYWORDS.add("INSERT");
    KEYWORDS.add("INTO");
    KEYWORDS.add("KILL");
    KEYWORDS.add("LABEL");
    KEYWORDS.add("LAST");
    KEYWORDS.add("LATEST");
    KEYWORDS.add("LEVEL");
    KEYWORDS.add("LIKE");
    KEYWORDS.add("LIMIT");
    KEYWORDS.add("LINEAR");
    KEYWORDS.add("LINK");
    KEYWORDS.add("KEYWORDS");
    KEYWORDS.add("LOAD");
    KEYWORDS.add("LOCK");
    KEYWORDS.add("MERGE");
    KEYWORDS.add("METADATA");
    KEYWORDS.add("NODES");
    KEYWORDS.add("NOW");
    KEYWORDS.add("OF");
    KEYWORDS.add("OFF");
    KEYWORDS.add("OFFSET");
    KEYWORDS.add("ON");
    KEYWORDS.add("ORDER");
    KEYWORDS.add("PARTITION");
    KEYWORDS.add("PASSWORD");
    KEYWORDS.add("PATHS");
    KEYWORDS.add("PREVIOUS");
    KEYWORDS.add("PREVIOUSUNTILLAST");
    KEYWORDS.add("PRIVILEGES");
    KEYWORDS.add("PROCESSKEYWORDS");
    KEYWORDS.add("PROPERTY");
    KEYWORDS.add("QUERIES");
    KEYWORDS.add("QUERY");
    KEYWORDS.add("READONLY");
    KEYWORDS.add("REGEXP");
    KEYWORDS.add("REMOVE");
    KEYWORDS.add("RENAME");
    KEYWORDS.add("RESAMPLE");
    KEYWORDS.add("REVOKE");
    KEYWORDS.add("ROLE");
    KEYWORDS.add("SCHEMA");
    KEYWORDS.add("SELECT");
    KEYWORDS.add("SET");
    KEYWORDS.add("SETTLE");
    KEYWORDS.add("SGLEVEL");
    KEYWORDS.add("SHOW");
    KEYWORDS.add("SLIMIT");
    KEYWORDS.add("SNAPSHOT");
    KEYWORDS.add("SOFFSET");
    KEYWORDS.add("STORAGE");
    KEYWORDS.add("START");
    KEYWORDS.add("STOP");
    KEYWORDS.add("SYSTEM");
    KEYWORDS.add("TAGS");
    KEYWORDS.add("TASK");
    KEYWORDS.add("TEMPLATE");
    KEYWORDS.add("TIMESERIES");
    KEYWORDS.add("TIMESTAMP");
    KEYWORDS.add("TO");
    KEYWORDS.add("TOLERANCE");
    KEYWORDS.add("TOP");
    KEYWORDS.add("TRACING");
    KEYWORDS.add("TRIGGER");
    KEYWORDS.add("TRIGGERS");
    KEYWORDS.add("TTL");
    KEYWORDS.add("UNLINK");
    KEYWORDS.add("UNLOAD");
    KEYWORDS.add("UNSET");
    KEYWORDS.add("UPDATE");
    KEYWORDS.add("UPSERT");
    KEYWORDS.add("USER");
    KEYWORDS.add("USING");
    KEYWORDS.add("VALUES");
    KEYWORDS.add("VERIFY");
    KEYWORDS.add("VERSION");
    KEYWORDS.add("WATERMARK_EMBEDDING");
    KEYWORDS.add("WHERE");
    KEYWORDS.add("WITH");
    KEYWORDS.add("WITHOUT");
    KEYWORDS.add("WRITABLE");
    KEYWORDS.add("BOOLEAN");
    KEYWORDS.add("DOUBLE");
    KEYWORDS.add("FLOAT");
    KEYWORDS.add("INT32");
    KEYWORDS.add("INT64");
    KEYWORDS.add("TEXT");
    KEYWORDS.add("DICTIONARY");
    KEYWORDS.add("DIFF");
    KEYWORDS.add("GORILLA");
    KEYWORDS.add("PLAIN");
    KEYWORDS.add("REGULAR");
    KEYWORDS.add("RLE");
    KEYWORDS.add("TS_2DIFF");
    KEYWORDS.add("GZIP");
    KEYWORDS.add("LZ4");
    KEYWORDS.add("SNAPPY");
    KEYWORDS.add("UNCOMPRESSED");
    KEYWORDS.add("SET_STORAGE_GROUP");
    KEYWORDS.add("CREATE_TIMESERIES");
    KEYWORDS.add("INSERT_TIMESERIES");
    KEYWORDS.add("READ_TIMESERIES");
    KEYWORDS.add("DELETE_TIMESERIES");
    KEYWORDS.add("CREATE_USER");
    KEYWORDS.add("DELETE_USER");
    KEYWORDS.add("MODIFY_PASSWORD");
    KEYWORDS.add("KEYWORDS_USER");
    KEYWORDS.add("GRANT_USER_PRIVILEGE");
    KEYWORDS.add("REVOKE_USER_PRIVILEGE");
    KEYWORDS.add("GRANT_USER_ROLE");
    KEYWORDS.add("REVOKE_USER_ROLE");
    KEYWORDS.add("CREATE_ROLE");
    KEYWORDS.add("DELETE_ROLE");
    KEYWORDS.add("KEYWORDS_ROLE");
    KEYWORDS.add("GRANT_ROLE_PRIVILEGE");
    KEYWORDS.add("REVOKE_ROLE_PRIVILEGE");
    KEYWORDS.add("CREATE_FUNCTION");
    KEYWORDS.add("DROP_FUNCTION");
    KEYWORDS.add("CREATE_TRIGGER");
    KEYWORDS.add("DROP_TRIGGER");
    KEYWORDS.add("START_TRIGGER");
    KEYWORDS.add("STOP_TRIGGER");
    KEYWORDS.add("CREATE_CONTINUOUS_QUERY");
    KEYWORDS.add("DROP_CONTINUOUS_QUERY");
    KEYWORDS.add("GOLBAL");
    // time 下回遇到时做个注解，1、insert sql中 插入time列
    // KEYWORDS.add("TIME");
    // KEYWORDS.add("ROOT");
  }

  public static boolean validateKeyWords(String key) {
    if (KEYWORDS.contains(key)) {
      return true;
    }
    return false;
  }
}
