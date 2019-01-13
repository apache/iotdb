/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.engine.overflow.metadata;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;

public class OFSeriesListMetadataTest {

    private final String path = "OFSeriesListMetadataTest";

    @Before
    public void setUp() throws Exception {

    }

    @After
    public void tearDown() throws Exception {
        File file = new File(path);
        if (file.exists()) {
            file.delete();
        }
    }

    @Test
    public void testOfSeriesListMetadataSerDe() throws Exception {
        OFSeriesListMetadata ofSeriesListMetadata = OverflowTestHelper.createOFSeriesListMetadata();
        serialized(ofSeriesListMetadata);
        OFSeriesListMetadata deOfSeriesListMetadata = deSerialized();
        // assert
        OverflowUtils.isOFSeriesListMetadataEqual(ofSeriesListMetadata, deOfSeriesListMetadata);
    }

    private void serialized(OFSeriesListMetadata obj) throws FileNotFoundException {
        FileOutputStream fileOutputStream = new FileOutputStream(path);
        try {
            obj.serializeTo(fileOutputStream);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                fileOutputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private OFSeriesListMetadata deSerialized() throws FileNotFoundException {
        FileInputStream fileInputStream = new FileInputStream(path);
        try {
            OFSeriesListMetadata ofSeriesListMetadata = OFSeriesListMetadata.deserializeFrom(fileInputStream);
            return ofSeriesListMetadata;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                fileInputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }
}