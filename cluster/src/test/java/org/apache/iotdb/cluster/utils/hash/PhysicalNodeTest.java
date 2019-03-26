/**
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
package org.apache.iotdb.cluster.utils.hash;

import static org.junit.Assert.*;

import org.apache.iotdb.cluster.utils.hash.PhysicalNode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PhysicalNodeTest {

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testEqualsObject1() {
    PhysicalNode n1 = new PhysicalNode("192.168.130.1", 6666);
    PhysicalNode n2 = new PhysicalNode("192.168.130.1", 6666);
    assertEquals(n1, n2);
    assertEquals(n1.getKey(), n2.getKey());
    assertEquals(n1.hashCode(), n2.hashCode());
  }

  @Test
  public void testEqualsObject2() {
    PhysicalNode n1 = new PhysicalNode(null, 6666);
    PhysicalNode n2 = new PhysicalNode(null, 6666);
    assertEquals(n1, n2);
    assertEquals(n1.getKey(), n2.getKey());
    assertEquals(n1.hashCode(), n2.hashCode());
  }
}
