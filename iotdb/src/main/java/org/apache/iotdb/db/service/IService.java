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
package org.apache.iotdb.db.service;

import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.StartupException;

public interface IService {
    /**
     * Start current service.
     */
    void start() throws StartupException;

    /**
     * Stop current service. If current service uses thread or thread pool, current service should guarantee to release
     * thread or thread pool.
     */
    void stop();

    /**
     * @return current service name
     */
    ServiceType getID();
}
