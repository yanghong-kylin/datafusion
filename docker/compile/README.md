# Guidance

## Build Image

> docker build -t hub.tess.io/kylin/datafusion-compile:1.0 -f ./compile/Dockerfile .

## Run Container

Run container with mounting local rust source code folder DataFusion
> docker run -v DataFusion:/tmp/DataFusion --name datafusion-compile -it hub.tess.io/kylin/datafusion-compile:1.0 bash

> docker start datafusion-compile

> docker exec -it datafusion-compile bash

## Compile

> scl enable llvm-toolset-7 bash
> clang --version

> cp -r /tmp/DataFusion ~/
> cd ~/DataFusion
> cargo build --release

## Copy Binary

> docker cp datafusion-compile:/root/DataFusion/target/release/datafusion-cli release-binaries/
> docker cp datafusion-compile:/root/DataFusion/target/release/ballista-scheduler release-binaries/
> docker cp datafusion-compile:/root/DataFusion/target/release/ballista-executor release-binaries/