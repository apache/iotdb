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

package org.apache.iotdb.db.query.udf.service;

import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.db.query.udf.api.UDFExecutorInterface;

import java.util.concurrent.ConcurrentHashMap;

public class UDFManagementService implements IService {
    private ConcurrentHashMap<String, UDFRegistrationInformation> registrationInformation;
    private ConcurrentHashMap<String, UDFExecutorInterface> udfExecutors;

    @Override
    public void start() throws StartupException {

    }

    @Override
    public void stop() {

    }

    @Override
    public ServiceType getID() {
        return null;
    }
}
