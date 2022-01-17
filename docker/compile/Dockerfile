FROM centos:7.3.1611

MAINTAINER DL-eBay-Kylin-Core <DL-eBay-Kylin-Core@ebay.com>

WORKDIR /tmp

# install system tools
RUN set -x \
    && yum install -y which \
       openssh-clients \
       unzip \
       sudo \
       net-tools \
       iftop \
       tcpdump \
       perf \
       telnet \
       lsof \
       wget \
       gcc \
       gcc-c++ \
       make \
    && yum clean all

# install jdk
RUN set -x \
    && yum install -y \
       java-1.8.0-openjdk \
       java-1.8.0-openjdk-devel \
    && yum clean all \
    && java -version

ENV JAVA_HOME /etc/alternatives/java_sdk

# install llvm
RUN set -x \
    && yum install -y centos-release-scl

RUN set -x \    
    && yum install -y llvm-toolset-7


# install hadoop clients
ARG HADOOP_VERSION=2.7.3.2.6.4-ebay.2

ARG APACHE_HOME=/apache

RUN set -x \
    && mkdir -p $APACHE_HOME \
    && ln -s $JAVA_HOME /apache/java

ENV HADOOP_HOME=$APACHE_HOME/hadoop
ADD dependency/hdp-clients/hadoop-${HADOOP_VERSION}.tar.gz $APACHE_HOME
RUN set -x \
    && ln -s $APACHE_HOME/hadoop-${HADOOP_VERSION} $HADOOP_HOME


# install Rust
RUN curl https://sh.rustup.rs -sSf | bash -s -- -y

# install git
RUN set -x \
    && yum install -y git \
    && git version \

ENV PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$HOME/.cargo/bin

RUN echo "export CLASSPATH=`hadoop classpath --glob`:$CLASSPATH" >> $HOME/.bashrc

ENV LD_LIBRARY_PATH=$JAVA_HOME/jre/lib/amd64/server/:$LD_LIBRARY_PATH