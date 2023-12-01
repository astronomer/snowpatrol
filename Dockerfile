FROM quay.io/astronomer/astro-runtime:9.6.0-python-3.10

COPY include/astro_provider_snowpark-0.0.0-py3-none-any.whl /tmp
RUN pip install /tmp/astro_provider_snowpark-0.0.0-py3-none-any.whl