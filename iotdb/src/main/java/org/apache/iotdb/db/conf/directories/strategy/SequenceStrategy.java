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
package org.apache.iotdb.db.conf.directories.strategy;

import java.util.List;

public class SequenceStrategy extends DirectoryStrategy {

    private int currentIndex;

    @Override
    public void init(List<String> folders) {
        super.init(folders);

        currentIndex = 0;
    }

    @Override
    public int nextFolderIndex() {
        int index = currentIndex;
        updateIndex();

        return index;
    }

    private void updateIndex() {
        currentIndex++;
        if (currentIndex >= folders.size())
            currentIndex = 0;
    }
}
