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

package org.apache.iotdb.tsfile.exception;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TsFileExceptionTest {

    private static final String MOCK = "mock";

    @Test
    public void testTsFileStatisticsMistakesException() {
        TsFileStatisticsMistakesException e = new TsFileStatisticsMistakesException(MOCK);
        assertEquals(MOCK, e.getMessage());
    }

    @Test
    public void testTsFileRuntimeException() {
        TsFileRuntimeException e = new TsFileRuntimeException(MOCK);
        assertEquals(MOCK, e.getMessage());
    }

    @Test
    public void testPathParseException() {
        PathParseException e = new PathParseException(MOCK);
        assertEquals("mock is not a legal path.", e.getMessage());
    }

    @Test
    public void testNullFieldException() {
        NullFieldException e = new NullFieldException();
        assertEquals("Field is null", e.getMessage());
    }

    @Test
    public void testNotImplementedException() {
        NotImplementedException e = new NotImplementedException(MOCK);
        assertEquals(MOCK, e.getMessage());
    }

    @Test
    public void testNotCompatibleTsFileException() {
        NotCompatibleTsFileException e = new NotCompatibleTsFileException(MOCK);
        assertEquals(MOCK, e.getMessage());
    }

    @Test
    public void testTsFileStatisticsMistakesException() {
        TsFileStatisticsMistakesException e = new TsFileStatisticsMistakesException(MOCK);
        assertEquals(MOCK, e.getMessage());
    }
    @Test
    public void testTsFileRuntimeException() {
        TsFileRuntimeException e = new TsFileRuntimeException(MOCK);
        assertEquals(MOCK, e.getMessage());
    }
    @Test
    public void testNotCompatibleTsFileException() {
        NotCompatibleTsFileException e = new NotCompatibleTsFileException(MOCK);
        assertEquals(MOCK, e.getMessage());
    }
    @Test
    public void testNotCompatibleTsFileException() {
        NotCompatibleTsFileException e = new NotCompatibleTsFileException(MOCK);
        assertEquals(MOCK, e.getMessage());
    }
    @Test
    public void testNotCompatibleTsFileException() {
        NotCompatibleTsFileException e = new NotCompatibleTsFileException(MOCK);
        assertEquals(MOCK, e.getMessage());
    }
    @Test
    public void testNotCompatibleTsFileException() {
        NotCompatibleTsFileException e = new NotCompatibleTsFileException(MOCK);
        assertEquals(MOCK, e.getMessage());
    }
    @Test
    public void testNotCompatibleTsFileException() {
        NotCompatibleTsFileException e = new NotCompatibleTsFileException(MOCK);
        assertEquals(MOCK, e.getMessage());
    }
    @Test
    public void testNotCompatibleTsFileException() {
        NotCompatibleTsFileException e = new NotCompatibleTsFileException(MOCK);
        assertEquals(MOCK, e.getMessage());
    }
    @Test
    public void testNotCompatibleTsFileException() {
        NotCompatibleTsFileException e = new NotCompatibleTsFileException(MOCK);
        assertEquals(MOCK, e.getMessage());
    }
    @Test
    public void testNotCompatibleTsFileException() {
        NotCompatibleTsFileException e = new NotCompatibleTsFileException(MOCK);
        assertEquals(MOCK, e.getMessage());
    }


}
