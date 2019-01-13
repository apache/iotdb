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

import java.util.ArrayList;
import java.util.List;

import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.utils.CommonUtils;

import org.apache.iotdb.db.exception.StartupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.iotdb.db.conf.IoTDBConstant;

public class StartupChecks {
    private static final Logger LOGGER = LoggerFactory.getLogger(StartupChecks.class);

    private final List<StartupCheck> preChecks = new ArrayList<>();
    private final List<StartupCheck> DEFALUT_TESTS = new ArrayList<>();

    public StartupChecks() {
        DEFALUT_TESTS.add(checkJMXPort);
        DEFALUT_TESTS.add(checkJDK);
    }

    public StartupChecks withDefaultTest() {
        preChecks.addAll(DEFALUT_TESTS);
        return this;
    }

    public StartupChecks withMoreTest(StartupCheck check) {
        preChecks.add(check);
        return this;
    }

    public void verify() throws StartupException {
        for (StartupCheck check : preChecks) {
            check.execute();
        }
    }

    public static final StartupCheck checkJMXPort = new StartupCheck() {

        @Override
        public void execute() throws StartupException {
            String jmxPort = System.getProperty(IoTDBConstant.REMOTE_JMX_PORT_NAME);
            if (jmxPort == null) {
                LOGGER.warn("JMX is not enabled to receive remote connection. "
                        + "Please check conf/{}.sh(Unix or OS X, if you use Windows, check conf/{}.bat) for more info",
                        IoTDBConstant.ENV_FILE_NAME, IoTDBConstant.ENV_FILE_NAME);
                jmxPort = System.getProperty(IoTDBConstant.TSFILEDB_LOCAL_JMX_PORT_NAME);
                if (jmxPort == null) {
                    LOGGER.warn("{} missing from {}.sh(Unix or OS X, if you use Windows, check conf/{}.bat)",
                            IoTDBConstant.TSFILEDB_LOCAL_JMX_PORT_NAME, IoTDBConstant.ENV_FILE_NAME,
                            IoTDBConstant.ENV_FILE_NAME);
                }
            } else {
                LOGGER.info("JMX is enabled to receive remote connection on port {}", jmxPort);
            }
        }
    };

    public static final StartupCheck checkJDK = new StartupCheck() {

        @Override
        public void execute() throws StartupException {
            int version = CommonUtils.getJDKVersion();
            if (version < IoTDBConstant.minSupportedJDKVerion) {
                throw new StartupException(String.format("Requires JDK version >= %d, current version is %d",
                        IoTDBConstant.minSupportedJDKVerion, version));
            } else {
                LOGGER.info("JDK veriosn is {}.", version);
            }
        }
    };
}
