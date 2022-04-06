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
package config;

import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.ServiceSpec;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;

@ControllerConfiguration
public class ConfigClusterReconciler implements Reconciler<ConfigCluster> {

  private final KubernetesClient client;
  private final Logger logger = LoggerFactory.getLogger(ConfigClusterReconciler.class);

  public ConfigClusterReconciler(KubernetesClient client) {
    this.client = client;
  }

  private final String ConfigClusterServiceName = "config-service";

  // construct a service
  //
  @Override
  public UpdateControl<ConfigCluster> reconcile(ConfigCluster configCluster, Context context) {
    // first ensures the service
    logger.info("detect whether the cluster has config node cluster service");
    Service clusterService =
        client
            .services()
            .inNamespace(configCluster.getMetadata().getNamespace())
            .withName(ConfigClusterServiceName)
            .get();
    if (clusterService == null) {
      logger.info("no service, constructing new service");
      Service configNodeService = new Service();
      configNodeService.setApiVersion("v1");
      ObjectMeta serviceMeta = new ObjectMeta();
      serviceMeta.setName(ConfigClusterServiceName);
      serviceMeta.setNamespace("iotdb");
      configNodeService.setMetadata(serviceMeta);
      ServiceSpec serviceSpec = new ServiceSpec();
      HashMap<String, String> selector = new HashMap<>();
      selector.put("app", "config");
      serviceSpec.setSelector(selector);
      ServicePort port = new ServicePort();
      port.setProtocol("TCP");
      port.setPort(80); // the internal port
      port.setTargetPort(new IntOrString(9376)); // the port seen by the cluster
      serviceSpec.setPorts(Collections.singletonList(port));
      configNodeService.setSpec(serviceSpec);
      client.services().create(configNodeService);
      return UpdateControl.noUpdate();
    }
    logger.info("has config node service");

    return UpdateControl.noUpdate();
  }
}
