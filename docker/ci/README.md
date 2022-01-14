# Guidance

## Build Image

> docker build -t hub.tess.io/kylin/datafusion-ci:1.0 -f ./Dockerfile .

## Run Container

Run container with mounting local rust source code folder DataFusion
> docker run -v DataFusion:/tmp/DataFusion --name datafusion-ci -it hub.tess.io/kylin/datafusion-ci:1.0 bash

> docker start datafusion-ci

> docker exec -it datafusion-ci bash

## Compile

> mkdir /tmp/output && CARGO_TARGET_DIR=/tmp/output cargo build --color=always --message-format=json-diagnostic-rendered-ansi --release --bin TestAggregation --manifest-path TestVectorization/Aggregation/Cargo.toml

## Copy Binary

> docker cp rust-package:/tmp/output/release/TestAggregation release-binaries