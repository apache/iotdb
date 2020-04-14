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
package org.apache.iotdb.cluster.log.manage.serializable;

import java.util.List;
import org.apache.iotdb.cluster.log.Log;

public interface LogDequeSerializer {

  /**
   * append a log
   * @param log appended log
   */
  public void append(Log log, LogManagerMeta meta);

  /**
   * append log list
   * @param logs appended log list
   */
  public void append(List<Log> logs, LogManagerMeta meta);

  /**
   * remove last log
   * @param meta metadata
   */
  public void removeLast(LogManagerMeta meta);

  /**
   * truncate num of logs
   * @param num num of logs
   * @param meta metadata
   */
  public void truncateLog(int num, LogManagerMeta meta);

  /**
   * remove 'num' of logs from first
   * @param num the number of removed logs
   */
  public void removeFirst(int num);

  /**
   * recover logs from disk
   * @return recovered logs
   */
  public List<Log> recoverLog();

  /**
   * recover meta from disk
   * @return recovered meta
   */
  public LogManagerMeta recoverMeta();

  /**
   * serialize meta of log manager
   * @param meta meta of log manager
   */
  public void serializeMeta(LogManagerMeta meta);

  /**
   * close file and release resource
   */
  public void close();
}
