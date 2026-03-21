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

package org.apache.iotdb.session.subscription.payload;

import org.apache.iotdb.rpc.subscription.annotation.TableModel;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionIncompatibleHandlerException;

import org.apache.thrift.annotation.Nullable;
import org.apache.tsfile.read.v4.ITsFileReader;
import org.apache.tsfile.read.v4.ITsFileTreeReader;
import org.apache.tsfile.read.v4.TsFileReaderBuilder;
import org.apache.tsfile.read.v4.TsFileTreeReaderBuilder;

import java.io.IOException;

public class SubscriptionTsFileHandler extends SubscriptionFileHandler {

  @Nullable private final String databaseName;

  public SubscriptionTsFileHandler(final String absolutePath, @Nullable final String databaseName) {
    super(absolutePath);
    this.databaseName = databaseName;
  }

  public ITsFileTreeReader openTreeReader() throws IOException {
    if (databaseName != null) {
      throw new SubscriptionIncompatibleHandlerException(
          String.format(
              "%s does not support openTreeReader() for table model tsfile.",
              getClass().getSimpleName()));
    }
    return new TsFileTreeReaderBuilder().file(getFile()).build();
  }

  @TableModel
  public ITsFileReader openTableReader() throws IOException {
    if (databaseName == null) {
      throw new SubscriptionIncompatibleHandlerException(
          String.format(
              "%s does not support openTableReader() for tree model tsfile.",
              getClass().getSimpleName()));
    }
    return new TsFileReaderBuilder().file(getFile()).build();
  }

  @TableModel
  public String getDatabaseName() {
    return databaseName;
  }
}
