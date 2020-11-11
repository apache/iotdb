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
package org.apache.iotdb.web.grafana.dao.impl;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.Assert.*;

public class BasicDaoImplTest {


    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void getInternal() {
        BasicDaoImpl impl = new BasicDaoImpl(null);
        ReflectionTestUtils.setField(impl, "isDownSampling", true);
        ReflectionTestUtils.setField(impl, "interval", "1m");

        String internal1 = impl.getInternal(0);
        assert internal1.equals("");

        String internal2 = impl.getInternal(3);
        System.out.println("123");
        System.out.println(internal2);
        assert internal2.equals("1m");

        String internal3 = impl.getInternal(25);
        assert internal3.equals("1h");

        String internal4 = impl.getInternal(24 * 30 + 1);
        assert internal4.equals("1d");
    }
}