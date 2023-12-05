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

package org.apache.iotdb.tsfile.read.filter;

import org.apache.iotdb.tsfile.file.metadata.IMetadata;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.TimeStatistics;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class FilterTestUtil {

  private FilterTestUtil() {
    // util class
  }

  public static IMetadata newMetadata(Statistics statistics) {
    return new IMetadata() {
      @Override
      public Statistics getStatistics() {
        return statistics;
      }

      @Override
      public Statistics getTimeStatistics() {
        return statistics;
      }

      @Override
      public Optional<Statistics<? extends Serializable>> getMeasurementStatistics(
          int measurementIndex) {
        return Optional.ofNullable(statistics);
      }

      @Override
      public boolean hasNullValue(int measurementIndex) {
        return false;
      }
    };
  }

  public static IMetadata newAlignedMetadata(
      TimeStatistics timeStatistics, Statistics<? extends Serializable> valueStatistics) {
    return new AlignedMetadata(timeStatistics, Collections.singletonList(valueStatistics));
  }

  private static class AlignedMetadata implements IMetadata {

    private final TimeStatistics timeStatistics;
    private final List<Statistics<? extends Serializable>> statisticsList;

    public AlignedMetadata(
        TimeStatistics timeStatistics, List<Statistics<? extends Serializable>> statisticsList) {
      this.timeStatistics = timeStatistics;
      this.statisticsList = statisticsList;
    }

    @Override
    public Statistics<? extends Serializable> getStatistics() {
      if (statisticsList.size() == 1 && statisticsList.get(0) != null) {
        return statisticsList.get(0);
      }
      return timeStatistics;
    }

    @Override
    public Statistics<? extends Serializable> getTimeStatistics() {
      return timeStatistics;
    }

    @Override
    public Optional<Statistics<? extends Serializable>> getMeasurementStatistics(
        int measurementIndex) {
      return Optional.ofNullable(statisticsList.get(measurementIndex));
    }

    @Override
    public boolean hasNullValue(int measurementIndex) {
      long rowCount = getTimeStatistics().getCount();
      Optional<Statistics<? extends Serializable>> statistics =
          getMeasurementStatistics(measurementIndex);
      return statistics.map(stat -> stat.hasNullValue(rowCount)).orElse(true);
    }
  }
}
