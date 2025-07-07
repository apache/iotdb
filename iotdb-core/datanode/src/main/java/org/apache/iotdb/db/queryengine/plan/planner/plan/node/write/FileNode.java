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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.write;

// TODO:[OBJECT] WAL serde
public class FileNode {

  private String filePath;

  private long offset;

  private byte[] content;

  private boolean isEOF;

  public FileNode(String filePath, boolean isEOF, long offset, byte[] content) {
    this.filePath = filePath;
    this.isEOF = isEOF;
    this.offset = offset;
    this.content = content;
  }

  public FileNode(boolean isEOF, long offset, byte[] content) {
    this.isEOF = isEOF;
    this.offset = offset;
    this.content = content;
  }

  public void setFilePath(String filePath) {
    this.filePath = filePath;
  }

  public String getFilePath() {
    return filePath;
  }

  public boolean isEOF() {
    return isEOF;
  }

  public byte[] getContent() {
    return content;
  }

  public long getOffset() {
    return offset;
  }
}
