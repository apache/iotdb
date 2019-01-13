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
package org.apache.iotdb.db.query.reader;

import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.db.utils.TimeValuePair;

import java.io.IOException;

/**
 * <p>
 * Vital read interface. Batch method is used to increase query speed. Get a batch data every time will be faster than
 * get one point every time.
 */
public interface IReader {

    boolean hasNext() throws IOException;

    TimeValuePair next() throws IOException;

    void skipCurrentTimeValuePair() throws IOException;

    void close() throws IOException;

    boolean hasNextBatch();

    BatchData nextBatch();

    BatchData currentBatch();
}
