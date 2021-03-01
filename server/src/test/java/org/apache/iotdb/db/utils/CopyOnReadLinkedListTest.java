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
package org.apache.iotdb.db.utils;

import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;

public class CopyOnReadLinkedListTest {

  @Test
  public void modifyListTest() {
    CopyOnReadLinkedList<String> slist = new CopyOnReadLinkedList<>();
    String str1 = "aaa";
    String str2 = "bbb";
    slist.add(str1);
    slist.add(str2);
    Iterator<String> iterator = slist.iterator();
    Assert.assertEquals(str1, iterator.next());
    Assert.assertEquals(str2, iterator.next());

    slist.reset();
    if (slist.contains(str1)) {
      slist.remove(str1);
    }
    Assert.assertEquals(1, slist.size());

    str2 = "ddd";
    // str2 in slist is not modified
    iterator = slist.iterator();
    Assert.assertNotEquals(str2, iterator.next());
  }

  @Test
  public void cloneModifiedListTest() {
    CopyOnReadLinkedList<String> slist = new CopyOnReadLinkedList<>();
    String str1 = "aaa";
    String str2 = "bbb";
    slist.add(str1);
    slist.add(str2);

    str2 = "ddd";
    // str2 in slist is not modified
    List<String> clist = slist.cloneList();
    Assert.assertEquals("aaa", clist.get(0));
    Assert.assertEquals("bbb", clist.get(1));
    Assert.assertFalse(clist.isEmpty());
  }
}
