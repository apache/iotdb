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

package org.apache.iotdb.session.subscription.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RetryUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(RetryUtils.class);
    public static final int MAX_RETRIES = 3;

    private RetryUtils() {}

    public static <E, T extends Exception> E retryOnException(CallableWithException<E, T> callable) throws T {
        int attempt = 0;
        while (attempt < MAX_RETRIES) {
            try {
                return callable.call();
            } catch (Exception e) {
                attempt++;
                if (attempt >= MAX_RETRIES) {
                    throw e;
                }
            }
        }
        return null;
    }


}
