#!/usr/bin/env bash
docker image build -t dask:latest .
docker tag dask:latest mattpaletta/dask:latest
docker push mattpaletta/dask:latest