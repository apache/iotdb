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
package org.apache.iotdb.commons.utils;

import org.apache.iotdb.commons.exception.MetadataException;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class PathUtilsTest {

  @Test
  public void testCheckSingleMeasurementsInPlace() throws MetadataException {
    List<String> measurements =
        new ArrayList<>(Arrays.asList("path_utils_test_s1", "`path_utils_test_s2`", "", null));

    PathUtils.checkIsLegalSingleMeasurementsAndUpdateInPlace(measurements);

    assertEquals("path_utils_test_s1", measurements.get(0));
    assertEquals("path_utils_test_s2", measurements.get(1));
    assertNull(measurements.get(2));
    assertNull(measurements.get(3));
  }

  @Test
  public void testCheckSingleMeasurementListsInPlaceReusesStrings() throws MetadataException {
    List<List<String>> measurementLists = new ArrayList<>();
    measurementLists.add(
        new ArrayList<>(
            Arrays.asList(new String("path_utils_batch_s1"), new String("`path_utils_batch_s2`"))));
    measurementLists.add(
        new ArrayList<>(
            Arrays.asList(new String("path_utils_batch_s1"), new String("`path_utils_batch_s2`"))));

    PathUtils.checkIsLegalSingleMeasurementListsAndUpdateInPlace(measurementLists);

    assertSame(measurementLists.get(0).get(0), measurementLists.get(1).get(0));
    assertSame(measurementLists.get(0).get(1), measurementLists.get(1).get(1));
    assertEquals("path_utils_batch_s2", measurementLists.get(0).get(1));
  }

  @Test
  public void testCheckSingleMeasurementListInPlace() throws MetadataException {
    List<String> measurements =
        new ArrayList<>(Arrays.asList("path_utils_batch_s1", "`path_utils_batch_s2`", null));

    PathUtils.checkIsLegalSingleMeasurementListsAndUpdateInPlace(
        Collections.singletonList(measurements));

    assertEquals("path_utils_batch_s1", measurements.get(0));
    assertEquals("path_utils_batch_s2", measurements.get(1));
    assertNull(measurements.get(2));
  }
}
