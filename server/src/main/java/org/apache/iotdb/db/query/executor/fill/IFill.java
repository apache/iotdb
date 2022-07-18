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

package org.apache.iotdb.db.query.executor.fill;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.SessionManager;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;

import java.io.IOException;
import java.util.Calendar;
import java.util.Set;

import static org.apache.iotdb.db.qp.utils.DatetimeUtils.MS_TO_MONTH;

public abstract class IFill {

  protected long queryStartTime;
  protected long queryEndTime;
  protected TSDataType dataType;

  protected boolean isBeforeByMonth = false;
  protected long beforeRange = 0;
  protected boolean isAfterByMonth = false;
  protected long afterRange = 0;

  public IFill(TSDataType dataType, long queryStartTime) {
    this.dataType = dataType;
    this.queryStartTime = queryStartTime;
  }

  public IFill() {}

  public abstract IFill copy();

  public abstract void configureFill(
      PartialPath path,
      TSDataType dataType,
      long queryTime,
      Set<String> deviceMeasurements,
      QueryContext context)
      throws QueryProcessException, StorageEngineException;

  public abstract TimeValuePair getFillResult()
      throws IOException, QueryProcessException, StorageEngineException;

  public TSDataType getDataType() {
    return this.dataType;
  }

  public void setDataType(TSDataType dataType) {
    this.dataType = dataType;
  }

  public void setQueryStartTime(long queryStartTime) {
    this.queryStartTime = queryStartTime;
  }

  public long getQueryStartTime() {
    return queryStartTime;
  }

  public long getQueryEndTime() {
    return queryEndTime;
  }

  abstract void constructFilter();

  public boolean insideBeforeRange(long previous, long startTime) {
    if (isBeforeByMonth) {
      return previous >= slideMonth(startTime, (int) (-beforeRange / MS_TO_MONTH));
    } else {
      return previous >= startTime - beforeRange;
    }
  }

  public boolean insideAfterRange(long next, long startTime) {
    if (isAfterByMonth) {
      return next <= slideMonth(startTime, (int) (afterRange / MS_TO_MONTH));
    } else {
      return next <= startTime + afterRange;
    }
  }

  public void convertRange(long startTime, long endTime) {
    if (beforeRange > 0) {
      if (isBeforeByMonth) {
        queryStartTime = slideMonth(startTime, (int) (-beforeRange / MS_TO_MONTH));
      } else {
        queryStartTime = startTime - beforeRange;
      }
    } else {
      queryStartTime = startTime;
    }

    if (afterRange > 0) {
      if (isAfterByMonth) {
        queryEndTime = slideMonth(endTime, (int) (afterRange / MS_TO_MONTH));
      } else {
        queryEndTime = endTime + afterRange;
      }
    } else {
      queryEndTime = endTime;
    }
  }

  public long getBeforeRange() {
    return beforeRange;
  }

  public void setBeforeRange(long beforeRange) {
    this.beforeRange = beforeRange;
  }

  public long getAfterRange() {
    return afterRange;
  }

  public void setAfterRange(long afterRange) {
    this.afterRange = afterRange;
  }

  protected long slideMonth(long startTime, int monthNum) {
    Calendar calendar = Calendar.getInstance();
    calendar.setTimeZone(SessionManager.getInstance().getCurrSessionTimeZone());
    calendar.setTimeInMillis(startTime);
    calendar.add(Calendar.MONTH, monthNum);
    return calendar.getTimeInMillis();
  }
}
