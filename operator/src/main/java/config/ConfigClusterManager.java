package config;

import io.fabric8.kubernetes.api.model.Context;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.ServiceSpec;
import io.fabric8.kubernetes.client.KubernetesClient;
import iotdb.IoTDBCluster;

import java.util.Collections;
import java.util.HashMap;

public class ConfigClusterManager {

  private final KubernetesClient client;

  public ConfigClusterManager(KubernetesClient client) {
    this.client = client;
  }

  public void Reconcile(IoTDBCluster cluster, Context context) {
    reconcileService(cluster, context);
  }

  private String serviceName(String clusterName) {
    return String.format("%s-config", clusterName);
  }

  private void reconcileService(IoTDBCluster cluster, Context context) {
    String namespace =  cluster.getMetadata().getNamespace();
    String clusterName = cluster.getMetadata().getName();

    // construct the declared config service
    Service configSpec = constructService(cluster, context);

    // get the cluster current config service
    Service current = client.services()
            .inNamespace(namespace)
            .withName(serviceName(clusterName))
            .get();

    // service not defined.
    if (current == null) {
      client.services().create(configSpec);
      return;
    }

    if (!current.equals(configSpec)) {
      client.services().createOrReplace(configSpec);
    }
  }

  private Service constructService(IoTDBCluster cluster, Context context) {
    String namespace = cluster.getMetadata().getNamespace();
    String clusterName = cluster.getMetadata().getName();

    // TODO use configuration maybe
    String configServiceName = serviceName(clusterName);
    HashMap<String, String> selectors = new HashMap<>();
    selectors.put("cluster", clusterName);
    selectors.put("app", "config");

    /* construct the default config service */
    Service configService = new Service();

    // 1. construct the ObjectMeta
    ObjectMeta serviceMeta = new ObjectMeta();
    serviceMeta.setName(configServiceName);
    serviceMeta.setName(namespace);
    serviceMeta.setLabels(selectors);


    // 2. construct the service spec
    ServiceSpec serviceSpec = new ServiceSpec();
    serviceSpec.setType("NodePort");

    ServicePort port = new ServicePort();
    port.setName("client");
    port.setPort(7616);
    port.setTargetPort(new IntOrString(7616));
    port.setProtocol("TCP");
    serviceSpec.setPorts(Collections.singletonList(port));

    serviceSpec.setSelector(selectors);


    // 3. override service fields with user-defined ServiceSpec
    Service udf = cluster.getSpec().getConfigCluster().getSpec().getConfigService();
    if (udf != null) {
      if (udf.getMetadata().getLabels() != null) {
        serviceMeta.setLabels(udf.getMetadata().getLabels());
      }
      if (udf.getSpec().getType() != null) {
        ServiceSpec udfSpec = udf.getSpec();
        serviceSpec.setType(udfSpec.getType());
        if (udfSpec.getLoadBalancerIP() != null) {
          serviceSpec.setLoadBalancerIP(udfSpec.getLoadBalancerIP());
        }
        if (udfSpec.getClusterIP() != null) {
          serviceSpec.setClusterIP(udfSpec.getClusterIP());
        }
      }
    }

    configService.setMetadata(serviceMeta);
    configService.setSpec(serviceSpec);

    return configService;
  }

  private void reconcileConfigNodeStatefulSet(IoTDBCluster cluster) {

  }
}
