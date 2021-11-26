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

package org.apache.iotdb.cluster.server.handlers.caller;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.query.dataset.ShowTimeSeriesResult;

import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** Handler for getting the schemas from each data group concurrently. */
public class ShowTimeSeriesHandler implements AsyncMethodCallback<List<ShowTimeSeriesResult>> {

  private static class ShowTimeSeriesResultComparator implements Comparator<ShowTimeSeriesResult> {

    @Override
    public int compare(ShowTimeSeriesResult o1, ShowTimeSeriesResult o2) {
      if (o1 == null && o2 == null) {
        return 0;
      } else if (o1 == null) {
        return -1;
      } else if (o2 == null) {
        return 1;
      }
      return o1.getName().compareTo(o2.getName());
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(ShowTimeSeriesHandler.class);

  /** String representation of a partial path for logging */
  private final String path;

  private final CountDownLatch countDownLatch;
  private final long startTimeInMs;

  private final Map<String, ShowTimeSeriesResult> timeSeriesNameToResult = new HashMap<>();
  private final List<Exception> exceptions = new ArrayList<>();

  public ShowTimeSeriesHandler(int numGroup, PartialPath path) {
    this.countDownLatch = new CountDownLatch(numGroup);
    this.path = path.toString();
    this.startTimeInMs = System.currentTimeMillis();
  }

  @Override
  public synchronized void onComplete(List<ShowTimeSeriesResult> response) {
    for (ShowTimeSeriesResult r : response) {
      timeSeriesNameToResult.put(r.getName(), r);
    }
    countDownLatch.countDown();
    logger.debug(
        "Got {} timeseries in path {}. Remaining count: {}",
        response.size(),
        path,
        countDownLatch.getCount());
  }

  @Override
  public synchronized void onError(Exception exception) {
    exceptions.add(exception);
    countDownLatch.countDown();
    logger.error("Failed to get timeseries in path {} because of {}", path, exception.getMessage());
  }

  public List<ShowTimeSeriesResult> getResult() throws MetadataException {
    if (!exceptions.isEmpty()) {
      MetadataException e =
          new MetadataException(
              "Exception happened when getting the result."
                  + " See the suppressed exceptions for causes.");
      for (Exception exception : exceptions) {
        e.addSuppressed(exception);
      }
      throw e;
    }

    // Wait for the results and ignore the interruptions.
    long timeout = IoTDBDescriptor.getInstance().getConfig().getQueryTimeoutThreshold();
    while (System.currentTimeMillis() - startTimeInMs < timeout) {
      try {
        if (countDownLatch.await(
            System.currentTimeMillis() - startTimeInMs, TimeUnit.MILLISECONDS)) {
          break;
        }
      } catch (InterruptedException ignored) {
      }
    }

    if (countDownLatch.getCount() != 0) {
      String errMsg =
          String.format(
              "Failed to get the show timeseries result"
                  + " since %d nodes didn't respond after %d ms",
              countDownLatch.getCount(), timeout);
      logger.error(errMsg);
      throw new MetadataException(errMsg);
    }

    return timeSeriesNameToResult.values().stream()
        .sorted(new ShowTimeSeriesResultComparator())
        .collect(Collectors.toList());
  }
}
