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

package org.apache.iotdb.db.schemaengine.table;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.IMemMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.view.TreeToTableViewReference;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Set;

public class TreeToTableViewCacheTest {

  /**
   * The cache is a singleton, so every case registers its own views and removes them again, and
   * every case uses its own database.
   */
  private final ITreeToTableViewCache viewCache = TreeToTableViewCache.getInstance();

  @Test
  public void testAddAlterAndRemoveView() throws Exception {
    final String database = "add_test";
    final PartialPath viewPath = new PartialPath("root.add_test.dev01.**");
    final PartialPath devicePath = new PartialPath("root.add_test.dev01");
    try {
      Assert.assertTrue(viewCache.getTreeToTableViews(devicePath).isEmpty());

      Assert.assertTrue(viewCache.addView(database, "view1", false, 0, viewPath));
      // the same view at the same path twice does not add a second reference
      Assert.assertFalse(viewCache.addView(database, "view1", false, 0, viewPath));
      Assert.assertTrue(isCovered(devicePath, "view1"));

      Assert.assertTrue(viewCache.alterViewName(database, "view2", "view1"));
      Assert.assertFalse(isCovered(devicePath, "view1"));
      Assert.assertTrue(isCovered(devicePath, "view2"));

      Assert.assertTrue(viewCache.removeView(database, "view2"));
      Assert.assertTrue(viewCache.getTreeToTableViews(devicePath).isEmpty());
      // the view is gone, so removing it again fails
      Assert.assertFalse(viewCache.removeView(database, "view2"));
    } finally {
      viewCache.removeView(database, "view1");
      viewCache.removeView(database, "view2");
    }
  }

  @Test
  public void testRemovingOneViewKeepsTheOthers() throws Exception {
    final String database = "remove_test";
    final PartialPath viewPath = new PartialPath("root.remove_test.dev01.**");
    final PartialPath devicePath = new PartialPath("root.remove_test.dev01");
    try {
      Assert.assertTrue(viewCache.addView(database, "view3", false, 0, viewPath));
      Assert.assertTrue(viewCache.addView(database, "view4", false, 0, viewPath));

      Assert.assertTrue(viewCache.removeView(database, "view3"));

      final Set<TreeToTableViewReference> views = viewCache.getTreeToTableViews(devicePath);
      Assert.assertEquals(1, views.size());
      Assert.assertEquals("view4", views.iterator().next().getTableName());
    } finally {
      viewCache.removeView(database, "view3");
      viewCache.removeView(database, "view4");
    }
  }

  /** The nodes created for the path of the removed view have to be dropped together with it. */
  @Test
  public void testRemoveViewPrunesTheTrie() throws Exception {
    final String database = "prune_test";
    final PartialPath viewPath = new PartialPath("root.prune_test.dev01.**");
    final PartialPath devicePath = new PartialPath("root.prune_test.dev01");
    try {
      Assert.assertTrue(viewCache.addView(database, "view5", false, 0, viewPath));

      Assert.assertTrue(viewCache.removeView(database, "view5"));

      // the path of the removed view is not held by the trie any more
      Assert.assertNull(getRootNode().getChild(database));
      Assert.assertTrue(viewCache.getTreeToTableViews(devicePath).isEmpty());
    } finally {
      viewCache.removeView(database, "view5");
    }
  }

  @Test
  public void testRemoveViewOfAnotherDatabase() throws Exception {
    final String database = "database_test";
    final PartialPath viewPath = new PartialPath("root.database_test.dev01.**");
    final PartialPath devicePath = new PartialPath("root.database_test.dev01");
    try {
      Assert.assertTrue(viewCache.addView(database, "view6", false, 0, viewPath));

      // the view of another database is another view, even if the name and the path are the same
      Assert.assertFalse(viewCache.removeView("another_database", "view6"));
      Assert.assertTrue(isCovered(devicePath, "view6"));
    } finally {
      viewCache.removeView(database, "view6");
    }
  }

  private boolean isCovered(final PartialPath devicePath, final String tableName) {
    return viewCache.getTreeToTableViews(devicePath).stream()
        .anyMatch(view -> tableName.equals(view.getTableName()));
  }

  private static IMemMNode getRootNode() throws Exception {
    final Field field = TreeToTableViewCache.class.getDeclaredField("rootNode");
    field.setAccessible(true);
    return (IMemMNode) field.get(TreeToTableViewCache.getInstance());
  }
}
