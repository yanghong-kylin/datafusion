FROM hub.tess.io/kubernetes/tess-build-jenkins:latest

MAINTAINER DL-eBay-Kylin-Core <DL-eBay-Kylin-Core@ebay.com>

WORKDIR /tmp

# install jdk
RUN set -x \
    && apt-get install -y openjdk-8-jdk \
    && java -version

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64

# install llvm
RUN set -x \
    && apt install -y lsb-release wget software-properties-common apt-transport-https \
    && cd /tmp \
    && wget https://apt.llvm.org/llvm.sh \
    && chmod +x llvm.sh \
    && ./llvm.sh 12


# install hadoop clients
ARG HADOOP_VERSION=2.7.3.2.6.4-ebay.2

ARG APACHE_HOME=/apache

RUN set -x \
    && mkdir -p $APACHE_HOME \
    && ln -s $JAVA_HOME /apache/java

ENV HADOOP_HOME=$APACHE_HOME/hadoop
ADD hdp-clients/hadoop-${HADOOP_VERSION}.tar.gz $APACHE_HOME
RUN set -x \
    && ln -s $APACHE_HOME/hadoop-${HADOOP_VERSION} $HADOOP_HOME


# install Rust
RUN curl https://sh.rustup.rs -sSf | bash -s -- -y

ENV PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$HOME/.cargo/bin

RUN echo "export CLASSPATH=`hadoop classpath --glob`:$CLASSPATH" >> $HOME/.bashrc

ENV LD_LIBRARY_PATH=$JAVA_HOME/jre/lib/amd64/server/:$LD_LIBRARY_PATH