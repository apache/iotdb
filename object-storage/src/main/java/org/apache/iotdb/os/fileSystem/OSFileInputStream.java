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

package org.apache.iotdb.os.fileSystem;

import java.io.IOException;
import java.io.InputStream;

public class OSFileInputStream extends InputStream {

  private OSInput osInput;
  private long position; // 15
  private long size; // 100
  private InputStream currentInputStream; // 第二个文件，从 5 开始

  public OSFileInputStream(OSInput osInput, long position, long size) {
    this.osInput = osInput;
    this.position = position;
    this.size = size;
  }

  @Override
  public int read() throws IOException {

    return 0;
  }
}
