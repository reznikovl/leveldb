FROM ubuntu:latest
ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
clang \
git \ 
vim \
make \
cmake \
gdb \

# RUN sudo apt-get install ant
# RUN wget http://www.apache.org/dyn/closer.cgi?path=/thrift/0.15.0/thrift-0.15.0.tar.gz
# RUN tar -xvf thrift-0.15.0.tar.gz
# RUN cd thrift-0.15.0
# RUN ./bootstrap.sh \
# ./configure \
# sudo make \
# sudo make install


# RUN ln -s /usr/bin/clang-9 /usr/bin/clang
# RUN ln -s /usr/bin/clang++-9 /usr/bin/clang++