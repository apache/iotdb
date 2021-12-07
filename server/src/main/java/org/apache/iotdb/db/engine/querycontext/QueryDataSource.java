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

package org.apache.iotdb.db.engine.querycontext;

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.operator.AndFilter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryDataSource {
  private List<TsFileResource> seqResources;
  private List<TsFileResource> unseqResources;

  private TsFileResource unclosedSeqResource;
  private TsFileResource unclosedunseqResource;

  /** data older than currentTime - dataTTL should be ignored. */
  private long dataTTL = Long.MAX_VALUE;

  private final Map<String, Integer[]> unSeqFileOrderIndexesMap;

  public QueryDataSource(List<TsFileResource> seqResources, List<TsFileResource> unseqResources) {
    this.seqResources = seqResources;
    this.unseqResources = unseqResources;
    this.unSeqFileOrderIndexesMap = new HashMap<>();
  }

  public List<TsFileResource> getSeqResources() {
    return seqResources;
  }

  public List<TsFileResource> getUnseqResources() {
    return unseqResources;
  }

  public long getDataTTL() {
    return dataTTL;
  }

  public Integer[] getUnSeqFileOrderIndexes(String deviceId) {
    return unSeqFileOrderIndexesMap.get(deviceId);
  }

  public void setUnclosedSeqResource(TsFileResource unclosedSeqResource) {
    this.unclosedSeqResource = unclosedSeqResource;
  }

  public void setUnclosedunseqResource(TsFileResource unclosedunseqResource) {
    this.unclosedunseqResource = unclosedunseqResource;
  }

  public void setDataTTL(long dataTTL) {
    this.dataTTL = dataTTL;
  }

  public void setUnSeqFileOrderIndexes(String deviceId, Integer[] indexes) {
    this.unSeqFileOrderIndexesMap.put(deviceId, indexes);
  }

  /** @return an updated filter concerning TTL */
  public Filter updateFilterUsingTTL(Filter filter) {
    if (dataTTL != Long.MAX_VALUE) {
      if (filter != null) {
        filter = new AndFilter(filter, TimeFilter.gtEq(System.currentTimeMillis() - dataTTL));
      } else {
        filter = TimeFilter.gtEq(System.currentTimeMillis() - dataTTL);
      }
    }
    return filter;
  }
}
