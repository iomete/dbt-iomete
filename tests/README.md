# Testing dbt-iomete


## Set environment variables
```bash
export DBT_IOMETE_HOST=dev.iomete.cloud
export DBT_IOMETE_PORT=443
export DBT_IOMETE_HTTPS=true
export DBT_IOMETE_LAKEHOUSE=dbt
export DBT_IOMETE_USER_NAME=admin
export DBT_IOMETE_DOMAIN=default
export DBT_IOMETE_TOKEN=<dbt-token>
export DBT_IOMETE_DATAPLANE=spark-resources
```

## Run integration test

```shell
tox -e integration-iomete
```

## Run functional test (Using DBT Adaptor Tests)

```shell
tox -e functional
```

## Run unit tests
```shell
tox -e unit
```