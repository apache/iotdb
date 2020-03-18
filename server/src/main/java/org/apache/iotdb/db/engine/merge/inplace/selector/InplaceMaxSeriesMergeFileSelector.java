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

package org.apache.iotdb.db.engine.merge.inplace.selector;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.merge.BaseFileSelector;
import org.apache.iotdb.db.engine.merge.IMergeFileSelector;
import org.apache.iotdb.db.engine.merge.MaxSeriesMergeFileSelector;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.MergeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MaxSeriesMergeFileSelector is an extension of IMergeFileSelector which tries to maximize the
 * number of timeseries that can be merged at the same time.
 */
public class InplaceMaxSeriesMergeFileSelector extends MaxSeriesMergeFileSelector {

  public InplaceMaxSeriesMergeFileSelector(BaseFileSelector baseSelector) {
    super(baseSelector);
  }

}
