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

package org.apache.iotdb.db.pipe.external;

import org.apache.iotdb.db.metadata.path.MeasurementPath;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/** {@link ExternalPipeTimeseriesGroup} represents a group of timeseries. */
public class ExternalPipeTimeseriesGroup {

  private final Set<MeasurementPath> measurementPaths;

  public ExternalPipeTimeseriesGroup(Collection<MeasurementPath> measurementPaths) {
    this.measurementPaths = new HashSet<>(measurementPaths);
  }

  public Collection<MeasurementPath> getMeasurementPaths() {
    return Collections.unmodifiableSet(measurementPaths);
  }

  public boolean has(MeasurementPath measurementPath) {
    return measurementPaths.contains(measurementPath);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ExternalPipeTimeseriesGroup that = (ExternalPipeTimeseriesGroup) o;
    return measurementPaths.equals(that.measurementPaths);
  }

  @Override
  public int hashCode() {
    return Objects.hash(measurementPaths);
  }
}
