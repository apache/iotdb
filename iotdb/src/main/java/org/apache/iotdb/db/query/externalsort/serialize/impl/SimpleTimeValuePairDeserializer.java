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
package org.apache.iotdb.db.query.externalsort.serialize.impl;

import org.apache.iotdb.db.query.externalsort.serialize.TimeValuePairDeserializer;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.db.query.externalsort.serialize.TimeValuePairDeserializer;

import java.io.*;

public class SimpleTimeValuePairDeserializer implements TimeValuePairDeserializer {

    private InputStream inputStream;
    private ObjectInputStream objectInputStream;
    private String tmpFilePath;

    public SimpleTimeValuePairDeserializer(String tmpFilePath) throws IOException {
        inputStream = new BufferedInputStream(new FileInputStream(tmpFilePath));
        objectInputStream = new ObjectInputStream(inputStream);
        this.tmpFilePath = tmpFilePath;
    }

    @Override
    public boolean hasNext() throws IOException {
        return inputStream.available() > 0;
    }

    @Override
    public TimeValuePair next() throws IOException {
        try {
            return (TimeValuePair) objectInputStream.readUnshared();
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
    }

    // @Override
    // public void skipCurrentTimeValuePair() throws IOException {
    // next();
    // }

    /**
     * This method will delete
     *
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        objectInputStream.close();
        File file = new File(tmpFilePath);
        if (!file.delete()) {
            throw new IOException("Delete external sort tmp file error. FilePath:" + tmpFilePath);
        }
    }
}
