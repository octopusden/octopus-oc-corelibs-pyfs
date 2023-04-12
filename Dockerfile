FROM ubuntu
RUN apt-get -y update && \
    apt-get -y install python3 python3-pip python3-pysvn
RUN rm -rf /build
COPY --chown=root:root . /build
WORKDIR /build
USER root
RUN python3 -m pip install $(pwd) && python3 -m unittest discover -v && python3 setup.py bdist_wheel
