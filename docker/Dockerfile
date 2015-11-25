FROM centos:7
MAINTAINER hdaikoku <daikoku@hpcs.cs.tsukuba.ac.jp>

# install required packages
RUN yum -y install git cmake gcc gcc-c++ make ruby automake libtool

# install msgpack-c
RUN cd /tmp \
 && git clone -b cpp-0.5 https://github.com/msgpack/msgpack-c.git \
 && cd msgpack-c \
 && cmake . \
 && make \
 && make install \
 && make clean

# install jubatus-mpio
RUN cd /tmp \
 && git clone https://github.com/jubatus/jubatus-mpio.git \
 && cd jubatus-mpio \
 && ./bootstrap \
 && ./configure \
 && make \
 && make install \
 && make clean

# install jubatus-msgpack-rpc
RUN cd /tmp \
 && git clone https://github.com/jubatus/jubatus-msgpack-rpc.git \
 && cd jubatus-msgpack-rpc/cpp \
 && ./bootstrap \
 && ./configure \
 && make \
 && make install \
 && make clean

# install Intel TBB
ENV TBB_VERSION 44_20151010
ENV TBB_URL https://www.threadingbuildingblocks.org/sites/default/files/software_releases/linux/tbb${TBB_VERSION}oss_lin_0.tgz
ENV TBB_DIR /usr/local
RUN cd /tmp \
 && curl -O ${TBB_URL} \
 && tar zxvf tbb${TBB_VERSION}oss_lin_0.tgz -C ${TBB_DIR} --strip=1 \
 && rm tbb${TBB_VERSION}oss_lin_0.tgz \
 && sed -i "s%SUBSTITUTE_INSTALL_DIR_HERE%${TBB_DIR}%" ${TBB_DIR}/bin/tbbvars.*

# copy private key for cloning private repository
RUN mkdir /root/.ssh/
ADD deploy-key /root/.ssh/id_rsa
RUN chmod 600 /root/.ssh/id_rsa \
 && chown -R root:root /root/.ssh \
 && touch /root/.ssh/known_hosts \
 && ssh-keyscan git.omni.hpcc.jp >> /root/.ssh/known_hosts

# to clone the latest codes from the remote repository,
# just add "--build-arg INVALIDATED=$(date +%s)" option when doing "docker build"
ARG INVALIDATED=false
# install rdd++
RUN cd /tmp \
 && git clone git@git.omni.hpcc.jp:daikoku/rdd.git \
 && cd rdd \
 && source ${TBB_DIR}/bin/tbbvars.sh intel64 \
 && cmake . \
 && make

WORKDIR /tmp/rdd
ENV LD_LIBRARY_PATH /usr/local/lib/intel64/gcc4.4:/usr/local/lib