#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

#set -e

repo="wangbichao/parquet-tpch"
version="0.0.1"
if [[ "$(docker images -q $repo 2> /dev/null)" == "" ]]; then
  docker pull $repo:$version
fi

filePath=`pwd`/data/region/_SUCCESS
if [ ! -f $filePath ];
then
  containerID=`docker run -d $repo:$version`
  docker cp $containerID:/root/tmp `pwd`
  docker rm -f $containerID >/dev/null
  mv tmp `pwd`/data/tpch-1g-oneFile
else
  echo "data exists."
fi
ls -l ./data/tpch-1g-oneFile