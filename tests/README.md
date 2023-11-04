# Testing dbt-iomete


## Set environment variables
```bash
export DBT_IOMETE_HOST=dev.iomete.com;
export DBT_IOMETE_PORT=443;
export DBT_IOMETE_LAKEHOUSE=lakehouse;
export DBT_IOMETE_USER_NAME=fuad;
export DBT_IOMETE_TOKEN=mjbwx2OntziNfv2VQCiltnSLQTrcbAaZFQwRhc6aUo0=;
```

## Run integration test

```shell
tox -e integration-iomete
```

## Run unit tests
```shell
tox -e unit
```