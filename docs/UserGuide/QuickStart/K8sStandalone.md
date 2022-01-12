<!--

```
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
```

-->

## Prerequisite

1. An available kubernetes。
2. An available [kubectl](https://kubernetes.io/docs/tasks/tools/) that can access to kubernetes. 
3. An available [Helm](https://helm.sh/zh/docs/intro/quickstart/) 。

## Steps

1. Download iotdb's source code，cd kubernetes/helm/standalone
2. Generally，you only need to modify the ***values.yaml***，if need do more configuration，please put the modified config files, e.g. iotdb-engine.properties, into conf directory

***Description of values.yaml***

```yaml
# The k8s namespace iotdb should running in, if not set, will use the "default" namespace
namespace: "iotdb"

# All ports you want to expose
ports:
- name: rpc
  port: 6667

# The docker image-name:version of iotdb
image: "<image_name>:<image_version>"

# The docker image pull strategy, must be one of IfNotPresent, Always, Never
imagePullPolicy: Always

# If pulling from private docker registry, you should set the secret of the registry.
# You can run the following command to get the final secret string
#  "kubectl create secret docker-registry regsecret --docker-server=<your-registry-server> --docker-username=<your-name> --docker-password=<your-pword> --dry-run -o yaml"
#imagePullSecrets: ""

# When deleting, it will wait some sconds before execute terminal operation, if not set, use default (0).
terminationGracePeriodSeconds: 10

# The resource limitation for iotdb server
cpu: 4
memory: 8Gi
disk: 20Gi

# The storageClassName for PersistenceVolume, which can create pv dynamically, it depends on your kubernetes provisioner
# If not set, you should create enough pvs for iotdb manually, more details about pv see:
# https://kubernetes.io/docs/concepts/storage/persistent-volumes/
#storageClassName: alicloud-disk-ssd

# The nginx-ingress-controller namespace, it depends on how and where you or your kubernetes provisioner installed the nginx-ingress-controller
# If you have no need to connect to iotdb from out of k8s, you needn't set this parameter.  
#ingressNamespace: kube-system

# If you want to expose service to outside of kubernetes, you can set this parameter to either "LoadBalancer" or "NodePort"
# For LoadBalancer, you must ensure that your kubernetes provisioner has the ability to provide LoadBalancer for k8s.
# The NodePort could work but is not recommended.
# More details see https://kubernetes.io/docs/concepts/services-networking/service/#publishing-services-service-types
#exposeService: LoadBalancer

```

3. After configuring, you can run the following command to start up iotdb:

```shell script
helm install iotdb . 
```

4. Then you can get pods' status like this:

```shell script
kubectl -n <your_namespace> get pod -o wide -w
```

5. It means the iotdb server started successfully When the STATUS column becomes Running and the READY column becomes 1/1：

```shell script
NAME      READY   STATUS    RESTARTS   AGE
iotdb-0   1/1     Running   0          51m
```

6. After the cluster stated, you can use do some tests as follows：

```shell script
## Attach into iotdb server
kubectl -n <your_namespace> exec -it iotdb-0 -- bash
## Connect from cli
root@iotdb-0:~# /iotdb/sbin/start-cli.sh -h iotdb-0.iotdb.<your_namespace>.svc.cluster.local
---------------------
Starting IoTDB Cli
---------------------
 _____       _________  ______   ______
|_   _|     |  _   _  ||_   _ `.|_   _ \
  | |   .--.|_/ | | \_|  | | `. \ | |_) |
  | | / .'`\ \  | |      | |  | | |  __'.
 _| |_| \__. | _| |_    _| |_.' /_| |__) |
|_____|'.__.' |_____|  |______.'|_______/  version 0.13.0-SNAPSHOT


IoTDB> login successfully

```

7. You can use this command to completely delete iotdb

```shell script
helm delete iotdb
```

## Connect to iotdb from out of kubernetes

You have two ways to connect to iotdb from outside of the kubernetes:

### 1. LoadBalancer or NodePort

1. Set the parameter ```exposeService``` in values.yaml to ***LoadBalancer*** or ***NodePort*** and install iotdb with helm.
2. Get the ```EXTERNAL-IP``` with which you can access to the iotdb server running in kubernetes:

```shell script
   kubectl -n <iotdb_namespace> get svc iotdb-service

   NAME            TYPE           CLUSTER-IP      EXTERNAL-IP      PORT(S)          AGE
   iotdb-service   LoadBalancer   192.168.60.99   110.155.241.100   6667:30007/TCP   86m

```

### 2. Ingress

If you want to connect to iotdb from somewhere outside of the kubernetes via [ingress](https://kubernetes.io/docs/concepts/services-networking/ingress/), then you should have a [nginx-ingress-controller](https://kubernetes.io/docs/concepts/services-networking/ingress-controllers/).

Unlike other kubernetes controllers, the nginx-ingress-controller are not built-in with kubernetes, but some k8s provider, e.g. aliyun may install it when creating a k8s cluster.

[This document](https://kubernetes.github.io/ingress-nginx/deploy/) may help you if you want to install the nginx-ingress-controller manually.

(And unlike other http/https services which could be exposed by simply creating an ingres, for tcp services like iotdb, you need do more works) 

After installed the nginx-ingress-controller, you should do the following steps:

1. create ```ingress-config.yaml```, then execute ```kubectl apply -f ingress-config.yaml```, the content of ```ingress-config.yaml``` is:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: tcp-services
  namespace: <namespace_of_nginx_ingress_controller>
data:
  6667: "<same with the namespace parameter in values.yaml>/iotdb:6667"
```
2. Edit the nginx-service(when install nginx-ingress-controller, this service will be created in the same namespace) to add an 6667 port.

Then, you can connect to the iotdb from out of kubernetes, enjoy it.

More details about expose tcp services please see [here](https://kubernetes.github.io/ingress-nginx/user-guide/exposing-tcp-udp-services/?spm=a2c6h.12873639.0.0.7d515383w4iLp9)
