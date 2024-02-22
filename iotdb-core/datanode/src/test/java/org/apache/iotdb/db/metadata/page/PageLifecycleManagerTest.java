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

package org.apache.iotdb.db.metadata.page;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.memory.page.PageLifecycleManager;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.ISchemaPage;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.pagemgr.PagePool;
import org.apache.iotdb.tsfile.utils.Pair;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PageLifecycleManagerTest {

  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private final PageLifecycleManager manager = PageLifecycleManager.getInstance();

  @Before
  public void setUp() throws Exception {
    manager.loadConfiguration(5, 2, 3);
  }

  @After
  public void tearDown() throws Exception {
    manager.clear();
    manager.loadConfiguration(
        config.getPbtreeCachePageNum() + config.getPbtreeBufferPageNum(),
        config.getPbtreeCachePageNum(),
        config.getPbtreeBufferPageNum());
  }

  // Test proportion control and fifo strategy
  @Test
  public void testFIFOCacheBufferProportion() {
    PagePool pool1 = mock(PagePool.class);
    manager.registerPagePool(1, pool1);
    List<ISchemaPage> pageList = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      ISchemaPage page = mock(ISchemaPage.class);
      when(page.getRefCnt()).thenReturn(new AtomicInteger(0));
      when(page.getPageIndex()).thenReturn(i);
      pageList.add(page);
      when(pool1.evict(page)).thenReturn(true);
      when(pool1.flush(page)).thenReturn(true);
    }
    for (int i = 0; i < 3; i++) {
      ISchemaPage page = pageList.get(i);
      manager.putCachedPage(1, page);
    }
    Assert.assertEquals(3, manager.getCachedContainer().size());
    Assert.assertEquals(0, manager.getVolatileContainer().size());
    for (int i = 3; i < 5; i++) {
      ISchemaPage page = pageList.get(i);
      manager.putVolatilePage(1, page);
    }
    Assert.assertEquals(3, manager.getCachedContainer().size());
    Assert.assertEquals(2, manager.getVolatileContainer().size());
    for (int i = 5; i < 7; i++) {
      ISchemaPage page = pageList.get(i);
      manager.putCachedPage(1, page);
    }
    Assert.assertEquals(3, manager.getCachedContainer().size());
    Assert.assertEquals(2, manager.getVolatileContainer().size());
    Iterator<Pair<Integer, ISchemaPage>> iterator = manager.getCachedContainer().iterator();
    Assert.assertEquals(2, iterator.next().right.getPageIndex());
    Assert.assertEquals(5, iterator.next().right.getPageIndex());
    Assert.assertEquals(6, iterator.next().right.getPageIndex());
    Assert.assertFalse(iterator.hasNext());

    manager.putVolatilePage(1, pageList.get(7));
    Assert.assertEquals(2, manager.getCachedContainer().size());
    Assert.assertEquals(3, manager.getVolatileContainer().size());
    manager.putVolatilePage(1, pageList.get(8));
    Assert.assertEquals(2, manager.getCachedContainer().size());
    Assert.assertEquals(3, manager.getVolatileContainer().size());
    manager.putCachedPage(1, pageList.get(9));
    Assert.assertEquals(2, manager.getCachedContainer().size());
    Assert.assertEquals(3, manager.getVolatileContainer().size());
    iterator = manager.getCachedContainer().iterator();
    Assert.assertEquals(6, iterator.next().right.getPageIndex());
    Assert.assertEquals(9, iterator.next().right.getPageIndex());
    Assert.assertFalse(iterator.hasNext());
    iterator = manager.getVolatileContainer().iterator();
    Assert.assertEquals(4, iterator.next().right.getPageIndex());
    Assert.assertEquals(7, iterator.next().right.getPageIndex());
    Assert.assertEquals(8, iterator.next().right.getPageIndex());
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testLockPage() {
    PagePool pool1 = mock(PagePool.class);
    PagePool pool2 = mock(PagePool.class);
    manager.registerPagePool(1, pool1);
    manager.registerPagePool(2, pool2);

    // [1, 1] is locked volatile page
    ISchemaPage page = mock(ISchemaPage.class);
    when(page.getRefCnt()).thenReturn(new AtomicInteger(1));
    when(page.getPageIndex()).thenReturn(1);
    when(pool1.flush(page)).thenReturn(false);
    manager.putVolatilePage(1, page);

    // other page is not locked
    for (int i = 0; i < 2; i++) {
      page = mock(ISchemaPage.class);
      when(page.getRefCnt()).thenReturn(new AtomicInteger(0));
      when(page.getPageIndex()).thenReturn(2 + i);
      when(pool2.flush(page)).thenReturn(true);
      manager.putVolatilePage(2, page);
    }
    for (int i = 0; i < 2; i++) {
      page = mock(ISchemaPage.class);
      when(page.getRefCnt()).thenReturn(new AtomicInteger(0));
      when(page.getPageIndex()).thenReturn(4 + i);
      when(pool1.evict(page)).thenReturn(true);
      manager.putCachedPage(1, page);
    }

    // check current status
    Iterator<Pair<Integer, ISchemaPage>> iterator = manager.getVolatileContainer().iterator();
    int idx = 1;
    while (iterator.hasNext()) {
      Pair<Integer, ISchemaPage> pair = iterator.next();
      Assert.assertEquals((idx == 2 || idx == 3) ? 2 : 1, pair.left.intValue());
      Assert.assertEquals(idx++, pair.right.getPageIndex());
    }
    iterator = manager.getCachedContainer().iterator();
    while (iterator.hasNext()) {
      Pair<Integer, ISchemaPage> pair = iterator.next();
      Assert.assertEquals(1, pair.left.intValue());
      Assert.assertEquals(idx++, pair.right.getPageIndex());
    }

    // add a new volatile page, the locked page should be evicted
    page = mock(ISchemaPage.class);
    when(page.getRefCnt()).thenReturn(new AtomicInteger(0));
    when(page.getPageIndex()).thenReturn(6);
    when(pool1.flush(page)).thenReturn(true);
    manager.putVolatilePage(1, page);
    iterator = manager.getVolatileContainer().iterator();
    Assert.assertEquals(1, iterator.next().right.getPageIndex());
    Assert.assertEquals(3, iterator.next().right.getPageIndex());
    Assert.assertEquals(6, iterator.next().right.getPageIndex());
    Assert.assertFalse(iterator.hasNext());
  }
}
