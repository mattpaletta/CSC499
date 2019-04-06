FROM ubuntu:18.04

COPY polymer-v4.9.tar.gz /
    #here you need put the script name here.

RUN apt-get -y update
RUN apt-get -y install software-properties-common build-essential

RUN apt-get -y update
RUN apt-get -y install python3 wget python3-pip gdal-bin python-gdal python3-gdal python-pyproj libhdf4-dev python3-h5py libgrib-api-dev libgrib2c-dev libnetcdf-dev netcdf-bin

RUN pip3 install matplotlib numpy pandas scipy cython pyproj
RUN pip3 install pyepr python-hdf4 glymur lxml netcdf4 h5netcdf dask

# RUN apt-get update && \
#    apt-get install -y software-properties-common && \
#    add-apt-repository ppa:webupd8team/java && \
#    apt-get update && \
#    echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections && \
#    apt-get -y --allow-unauthenticated install oracle-java8-installer \
#    openjdk-8-jdk

RUN wget -nc https://hdfeos.org/software/pyhdf/pyhdf-0.9.0.tar.gz && tar zxf pyhdf-0.9.0.tar.gz
WORKDIR pyhdf-0.9.0
RUN python3 setup.py install && \
    python3 -m wheel convert /pyhdf-0.9.0/dist/pyhdf-0.9.0-py3.6-linux-x86_64.egg && \
    pip3 install /pyhdf-0.9.0/pyhdf-0.9.0-cp36-none-linux_x86_64.whl

WORKDIR /
RUN tar zxf polymer-v4.9.tar.gz
WORKDIR polymer-v4.9
RUN make auxdata_all && \
    python3 setup.py build_ext --inplace && \
    make ancillary && \
    pip3 install filelock
