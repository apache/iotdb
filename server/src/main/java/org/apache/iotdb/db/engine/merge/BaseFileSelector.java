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

package org.apache.iotdb.db.engine.merge;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.MergeException;
import org.apache.iotdb.db.utils.MergeUtils;
import org.apache.iotdb.db.utils.UpgradeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseFileSelector implements IMergeFileSelector {

  public abstract void select() throws MergeException;

  public abstract void select(boolean useTightBound) throws MergeException, IOException;

  public abstract void setConcurrentMergeNum(int concurrentMergeNum);

  public abstract MergeResource getResource();

  public abstract long getTotalCost();

  public abstract void selectOverlappedSeqFiles(TsFileResource unseqFile);

  public abstract void updateCost(long newCost, TsFileResource unseqFile);

  protected abstract static class TmpSelectedSeqIterable implements Iterable<Integer> {

  }
}