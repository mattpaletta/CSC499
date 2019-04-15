FROM ubuntu:18.04

COPY polymer-v4.9.tar.gz /
    #here you need put the script name here.

RUN apt-get -y update
RUN apt-get -y install software-properties-common build-essential

RUN apt-get -y update
RUN apt-get -y install python3 wget python3-pip

RUN apt-get -y install gdal-bin python-gdal python3-gdal python-pyproj libhdf4-dev python3-h5py libgrib-api-dev libgrib2c-dev libnetcdf-dev netcdf-bin
RUN apt-get -y update

RUN pip3 install jupyter matplotlib numpy pandas scipy
RUN pip3 install pyepr
RUN pip3 install cython pyproj
RUN pip3 install python-hdf4 glymur lxml
RUN pip3 install netcdf4
RUN pip3 install h5netcdf
RUN pip3 install matplotlib numpy pandas scipy cython pyproj

RUN pip3 install azure dask[distributed] dask[scheduler]

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

RUN cp -f /usr/bin/python3 /usr/bin/python
