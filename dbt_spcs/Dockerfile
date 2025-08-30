# Dockerfile
FROM ghcr.io/dbt-labs/dbt-snowflake:1.9.0
WORKDIR /app
COPY profiles.yml /root/.dbt/profiles.yml
COPY . /app
ENTRYPOINT ["dbt"]
CMD ["build"]
