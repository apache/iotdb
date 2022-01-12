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

## 前提条件

1. 有一个可用的kubernetes集群。
2. 有一个可以连接至kubernetes集群的[kubectl](https://kubernetes.io/docs/tasks/tools/) 客户端.
3. 有一个可用的[Helm](https://helm.sh/zh/docs/intro/quickstart/) 。

## 安装步骤

1. 下载iotdb源码，进入kubernetes/helm/standalone目录
2. 一般情况下，我们只需要修改values.yaml中的配置即可，如有特殊需要，可以把需要修改的iotdb的配置文件（如iotdb-engine.properties）放到conf目录下

### values配置文件

```yaml
# 运行iotdb的k8s命名空间，如果不设置将使用默认命名空间："default"
namespace: "iotdb"

# 所有想暴露的端口及端口名，以下三个是必选的，如有需要可以再后面继续追加
ports:
- name: rpc
  port: 6667

# iotdb的docker镜像名:版本号
image: "<image_name>:<image_version>"

# docker镜像拉取策略，可选项为： IfNotPresent, Always, Never
imagePullPolicy: Always

# 如果需要从私有镜像仓库拉取镜像，需要配置"imagePullSecrets"
#  可以运行下面的命令获取secret：
#  "kubectl create secret docker-registry regsecret --docker-server=<your-registry-server> --docker-username=<your-name> --docker-password=<your-pword> --dry-run -o yaml"
#imagePullSecrets: ""

# 当停止iotdb时，会延迟一段时间再停止服务，如果不设置该参数，默认为0，立即停止服务
terminationGracePeriodSeconds: 10

# 每个节点所需的资源
cpu: 4
memory: 8Gi
disk: 20Gi

# storageClassName可以为iotdb提供动态创建PersistentVolume的能力，具体值可以询问您的k8s云服务商
# 当然，您也可以不设置这个值，而是手动事先创建足够的PV供iotdb使用，更多关于PersistentVolume的细节可以参考：
# https://kubernetes.io/docs/concepts/storage/persistent-volumes/
#storageClassName: alicloud-disk-ssd

```

3. 在kubernetes/helm/standalone目录下运行以下命令即可启动iotdb

```shell script
helm install iotdb . 
```

4. 通过以下命令观察iotdb的启动状态

```shell script
kubectl -n <your_namespace> get pod -o wide -w
```

5. 当pod都进入ready状态后，表示启动成功：

```shell script
NAME      READY   STATUS    RESTARTS   AGE
iotdb-0   1/1     Running   0          51m
```

6. 通过cli访问iotdb：

```shell script
## 运行一下命令进入iotdb
kubectl -n <your_namespace> exec -it iotdb-0 -- bash
## 通过cli连接至本节点
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

7. 销毁iotdb

```shell script
helm delete iotdb
```

## 从kubernetes外部访问iotdb

有两种办法可以从kubernetes集群外部访问iotdb server。

### 1. LoadBalancer 或者 NodePort

1. 将values.yaml中的```exposeService```参数设置为```LoadBalancer```或者```NodePort```，然后用helm启动iotdb
2. 获取```EXTERNAL-IP```，通过这个ip就可以访问kubernetes集群内部的iotdb了。

```shell script
   kubectl -n <iotdb_namespace> get svc iotdb-service

   NAME            TYPE           CLUSTER-IP      EXTERNAL-IP      PORT(S)          AGE
   iotdb-service   LoadBalancer   192.168.60.99   110.155.241.100   6667:30007/TCP   86m

```

### 2. Ingress

如果需要通过[ingress](https://kubernetes.io/docs/concepts/services-networking/ingress/) 从kubernetes集群外部访问iotdb，那么你需要安装一个[nginx-ingress-controller](https://kubernetes.io/docs/concepts/services-networking/ingress-controllers/) 。

不同于其他kubernetes自带的controller，```nginx-ingress-controller```并不是kubernetes内置的，需要我们手动安装，但是，也有一些云厂商（比如阿里云）在创建kubernetes集群的时候顺便安装了```nginx-ingress-controller```.

```nginx-ingress-controller```的安装文档戳[这里](https://kubernetes.github.io/ingress-nginx/deploy/) 

（和其他http/https应用不同，对于iotdb这种tcp协议的服务，我们无法通过创建一个简单地```ingress```将服务暴露出去）

安装完成```nginx-ingress-controller```之后，ingress-controller会获得一个外部（公网）ip（绑定在Nodeport或者LoadBalance上），在使用这个ip还需要按照下面的步骤进行一些操作

1. 创建一个ingress-configmap.yaml, 然后执行```kubectl apply -f ingress-configmap.yaml```,ingress-configmap.yaml的内容如下：

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: tcp-services
  namespace: <namespace_of_nginx_ingress_controller>
data:
  6667: "<values.yaml文件中的namespace参数>/iotdb:6667"
```

2. 修改nginx-service(安装```nginx-ingress-controller```时会创建这个service，和```nginx-ingress-controller```在同一个namespace下)，添加6667端口。

做完上面散步操作之后，就可以从kubernetes外部访问iotdb了。

更多关于如何通过nginx-ingress-controller暴露tcp服务的细节可以参考[这里](https://kubernetes.github.io/ingress-nginx/user-guide/exposing-tcp-udp-services/?spm=a2c6h.12873639.0.0.7d515383w4iLp9) 。

