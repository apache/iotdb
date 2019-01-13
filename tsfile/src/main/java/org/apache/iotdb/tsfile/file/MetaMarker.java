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
package org.apache.iotdb.tsfile.file;

import java.io.IOException;

/**
 * MetaMarker denotes the type of headers and footers. Enum is not used for space saving.
 */
public class MetaMarker {
    public static final byte ChunkGroupFooter = 0;
    public static final byte ChunkHeader = 1;
    public static final byte Separator = 2;

    public static void handleUnexpectedMarker(byte marker) throws IOException {
        throw new IOException("Unexpected marker " + marker);
    }
}
