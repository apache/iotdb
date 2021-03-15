# !/bin/bash
path=`pwd`
docker run -v $path:/data thrift bash -c 'thrift -r -o /data --gen go /data/cluster.thrift'
docker run -v $path:/data thrift thrift -o /data/ --gen go /data/sync.thrift
