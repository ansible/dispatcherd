FROM python:3.12-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates unixodbc \
    && curl -fsSL https://packages.microsoft.com/config/debian/12/packages-microsoft-prod.deb -o /tmp/ms.deb \
    && dpkg -i /tmp/ms.deb && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 \
    && rm -rf /var/lib/apt/lists/* /tmp/ms.deb
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
# The development build excludes Git metadata from its context.
ENV SETUPTOOLS_SCM_PRETEND_VERSION_FOR_DISPATCHERD=0.0.dev0
WORKDIR /build
COPY pyproject.toml README.md LICENSE ./
COPY dispatcherd ./dispatcherd
RUN pip install --no-cache-dir '.[sqlserver]' pytest pytest-asyncio 'Django>=5.2,<5.3' 'mssql-django>=1.6,<3'
# Run against the installed package, with no source package in the working directory.
WORKDIR /tests
COPY tests/sqlserver ./sqlserver
COPY tools/sqlserver/django_publish.py ./sqlserver/django_publish.py
CMD ["pytest", "-q", "-o", "asyncio_mode=strict", "-o", "asyncio_default_fixture_loop_scope=function", "--confcutdir=/tests/sqlserver", "/tests/sqlserver"]
