FROM ghcr.io/linuxcontainers/alpine:latest

WORKDIR /dispatcherd

RUN apk add --no-cache python3 py3-pip && ln -sf python3 /usr/bin/python

RUN python3 -m venv /dispatcherd/venv

RUN echo $pwd

# Git is necessary because of setuptools-scm
RUN apk add --no-cache git

COPY . ./src

RUN /dispatcherd/venv/bin/pip3 install -e ./src[pg_notify] --verbose

COPY dispatcher.yml /dispatcherd/dispatcher1.yml
COPY dispatcher.yml /dispatcherd/dispatcher2.yml

RUN sed -i 's/\(node_id: \)demo-server-a/\1demo-server-1/' /dispatcherd/dispatcher1.yml
RUN sed -i 's/\(node_id: \)demo-server-a/\1demo-server-2/' /dispatcherd/dispatcher2.yml

RUN sed -i 's/\(host=\)localhost \(port=\)55777/\1msg_postgres \25432/' /dispatcherd/dispatcher1.yml
RUN sed -i 's/\(host=\)localhost \(port=\)55777/\1msg_postgres \25432/' /dispatcherd/dispatcher2.yml

