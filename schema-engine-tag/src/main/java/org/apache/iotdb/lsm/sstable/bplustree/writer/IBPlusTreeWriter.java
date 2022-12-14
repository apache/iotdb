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
package org.apache.iotdb.lsm.sstable.bplustree.writer;

import org.apache.iotdb.lsm.sstable.bplustree.entry.BPlusTreeEntry;
import org.apache.iotdb.lsm.sstable.bplustree.entry.BPlusTreeHeader;

import java.io.IOException;
import java.util.Map;
import java.util.Queue;

/** Used to generate a b+ tree for records and write to disk */
public interface IBPlusTreeWriter {

  /**
   * generate a b+ tree and header for records and write to disk
   *
   * @param records a map that holds all records, the map can be unordered
   * @return start offset of the b+ tree
   * @throws IOException
   */
  long write(Map<String, Long> records) throws IOException;

  /**
   * generate a b+ tree for records and write to disk
   *
   * @param records a map that holds all records, the map can be unordered
   * @return b+ tree header
   * @throws IOException
   */
  BPlusTreeHeader writeBPlusTree(Map<String, Long> records) throws IOException;

  /**
   * generate a b+ tree and header for records and write to disk
   *
   * @param records a queue that holds all records, the queue can be unordered
   * @return start offset of the b+ tree
   * @throws IOException
   */
  long write(Queue<BPlusTreeEntry> records) throws IOException;

  /**
   * generate a b+ tree for records and write to disk
   *
   * @param records a queue that holds all records, the queue can be unordered
   * @return b+ tree header
   * @throws IOException
   */
  BPlusTreeHeader writeBPlusTree(Queue<BPlusTreeEntry> records) throws IOException;

  /**
   * collect the records to be written to the disk, and only call write or writeBPlusTree to
   * actually write to the disk
   *
   * @param name name of the record
   * @param offset offset of the record
   * @return this
   */
  IBPlusTreeWriter collectRecord(String name, long offset);

  /**
   * generate a b+ tree and header for records and write to disk
   *
   * @return start offset of the b+ tree
   * @throws IOException
   */
  long write() throws IOException;

  /**
   * generate a b+ tree for records and write to disk
   *
   * @return b+ tree header
   * @throws IOException
   */
  BPlusTreeHeader writeBPlusTree() throws IOException;

  /**
   * close resource
   *
   * @throws IOException
   */
  void close() throws IOException;
}
