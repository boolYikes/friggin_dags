FROM apache/airflow:2.9.1

# for pkm use
USER root

# providers
RUN apt update && \
    apt install -y --no-install-recommends git && \
    git clone https://github.com/datavorous/YARS.git /tmp/YARS && \
    mkdir -p /home/airflow/.cache/uv

# crawler script
COPY ./crawl_with_proxy.py /tmp/YARS/src/crawl_with_proxy.py

# access control
# chmod is not redundant!
RUN chown -R airflow: /tmp && \
    chmod -R 777 /tmp

# for pip commands
USER airflow

# other providers and dependencies
RUN pip install --no-cache-dir -q yfinance pandas numpy requests Pygments aiohttp aiodns maxminddb dbt-redshift && \
    # pip install -U git+https://github.com/boolYikes/ProxyBroker.git && \
    # pip install attrs==23.2.0 && \ 
    pip install --no-cache-dir vaderSentiment

