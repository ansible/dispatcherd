ARG PYTHON_VERSION=3.12
FROM python:${PYTHON_VERSION}-slim-bookworm
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
RUN pip install --no-cache-dir '.[sqlserver]' pytest pytest-asyncio 'psycopg[binary]' 'Django>=5.2,<5.3' 'mssql-django>=1.6,<3'
# Run against the installed package, with no source package in the working directory.
WORKDIR /tests
COPY tests ./tests
COPY tools/sqlserver/django_publish.py ./tests/sqlserver/django_publish.py
COPY asyncio_tests ./asyncio_tests
COPY conftest.py ./conftest.py
COPY tools/sqlserver/run_tests.py ./run_tests.py
CMD ["python", "/tests/run_tests.py"]
