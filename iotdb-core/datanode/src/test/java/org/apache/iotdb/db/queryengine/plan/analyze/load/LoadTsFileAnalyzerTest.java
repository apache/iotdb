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

package org.apache.iotdb.db.queryengine.plan.analyze.load;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.LoadAnalyzeException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.PlainDeviceID;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.mockito.Mockito.mock;

public class LoadTsFileAnalyzerTest {

  @Test
  public void testSchemaVerifierShouldRejectDeviceWithEmptyPathNode() throws Exception {
    final File tsFile = File.createTempFile("load-tree-illegal-device", ".tsfile");

    try (final LoadTsFileAnalyzer analyzer = createAnalyzer(tsFile)) {
      final Object verifier = getSchemaAutoCreatorAndVerifier(analyzer);
      final Object schemaCache = getSchemaCache(verifier);
      addTimeSeries(
          schemaCache, new PlainDeviceID("root."), new MeasurementSchema("s1", TSDataType.INT32));

      final InvocationTargetException exception =
          Assert.assertThrows(
              InvocationTargetException.class,
              () -> getAutoCreateDatabaseMethod(verifier).invoke(verifier));
      Assert.assertTrue(
          String.valueOf(exception.getCause()),
          exception.getCause() instanceof LoadAnalyzeException);
    } finally {
      Assert.assertTrue(tsFile.delete());
    }
  }

  @Test
  public void testSchemaVerifierShouldIgnoreLegacyDatabaseWithEmptyPathNode() throws Exception {
    final File tsFile = File.createTempFile("load-tree-legacy-database", ".tsfile");

    try (final LoadTsFileAnalyzer analyzer = createAnalyzer(tsFile)) {
      final Object verifier = getSchemaAutoCreatorAndVerifier(analyzer);
      final PartialPath database = new PartialPath("root.sg");
      final PartialPath databaseWithSameStringPrefix = new PartialPath("root.sg1");
      final Set<PartialPath> databasesNeededToBeSet =
          new HashSet<>(Arrays.asList(database, databaseWithSameStringPrefix));

      getFilterAlreadySetDatabasesMethod(verifier)
          .invoke(verifier, databasesNeededToBeSet, Collections.singleton("root."));

      Assert.assertEquals(
          new HashSet<>(Arrays.asList(database, databaseWithSameStringPrefix)),
          databasesNeededToBeSet);
      Assert.assertTrue(getAlreadySetDatabases(getSchemaCache(verifier)).isEmpty());

      getFilterAlreadySetDatabasesMethod(verifier)
          .invoke(verifier, databasesNeededToBeSet, Collections.singleton(database.getFullPath()));

      Assert.assertEquals(
          Collections.singleton(databaseWithSameStringPrefix), databasesNeededToBeSet);
      Assert.assertEquals(
          Collections.singleton(database), getAlreadySetDatabases(getSchemaCache(verifier)));
    } finally {
      Assert.assertTrue(tsFile.delete());
    }
  }

  private LoadTsFileAnalyzer createAnalyzer(final File tsFile) throws Exception {
    return new LoadTsFileAnalyzer(
        LoadTsFileStatement.createUnchecked(tsFile.getAbsolutePath()),
        new MPPQueryContext(new QueryId("load_tree_test")),
        mock(IPartitionFetcher.class),
        mock(ISchemaFetcher.class));
  }

  private Object getSchemaAutoCreatorAndVerifier(final LoadTsFileAnalyzer analyzer)
      throws Exception {
    final Field field = LoadTsFileAnalyzer.class.getDeclaredField("schemaAutoCreatorAndVerifier");
    field.setAccessible(true);
    return field.get(analyzer);
  }

  private Object getSchemaCache(final Object verifier) throws Exception {
    final Field field = verifier.getClass().getDeclaredField("schemaCache");
    field.setAccessible(true);
    return field.get(verifier);
  }

  private void addTimeSeries(
      final Object schemaCache, final IDeviceID device, final MeasurementSchema measurementSchema)
      throws Exception {
    final Method method =
        schemaCache
            .getClass()
            .getDeclaredMethod("addTimeSeries", IDeviceID.class, MeasurementSchema.class);
    method.setAccessible(true);
    method.invoke(schemaCache, device, measurementSchema);
  }

  @SuppressWarnings("unchecked")
  private Set<PartialPath> getAlreadySetDatabases(final Object schemaCache) throws Exception {
    final Method method = schemaCache.getClass().getDeclaredMethod("getAlreadySetDatabases");
    method.setAccessible(true);
    return (Set<PartialPath>) method.invoke(schemaCache);
  }

  private Method getAutoCreateDatabaseMethod(final Object verifier) throws Exception {
    final Method method = verifier.getClass().getDeclaredMethod("autoCreateDatabase");
    method.setAccessible(true);
    return method;
  }

  private Method getFilterAlreadySetDatabasesMethod(final Object verifier) throws Exception {
    final Method method =
        verifier.getClass().getDeclaredMethod("filterAlreadySetDatabases", Set.class, Set.class);
    method.setAccessible(true);
    return method;
  }
}
