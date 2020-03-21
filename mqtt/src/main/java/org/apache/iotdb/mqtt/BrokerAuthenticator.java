/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.mqtt;

import io.moquette.broker.security.IAuthenticator;
import org.apache.commons.lang.StringUtils;

/**
 * The MQTT broker authenticator.
 */
public class BrokerAuthenticator implements IAuthenticator {
    private String username;
    private String password;

    public BrokerAuthenticator(String username, String password) {
        this.username = username;
        this.password = password;
    }

    @Override
    public boolean checkValid(String clientId, String username, byte[] password) {
        if (StringUtils.isNotBlank(username) && password != null) {
            return this.username.equals(username) && this.password.equals(new String(password));
        }
        return false;
    }
}
