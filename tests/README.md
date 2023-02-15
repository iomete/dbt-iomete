# Testing dbt-iomete


## Set environment variables
```bash
export DBT_IOMETE_WORKSPACE_ID=abcde-123
export DBT_IOMETE_HOST_NAME=<cluster_id>.iomete.cloud
export DBT_IOMETE_LAKEHOUSE=virtual-warehouse-1
export DBT_IOMETE_USER_NAME=<username>
export DBT_IOMETE_TOKEN=<user_access_token>
```

## Run integration test

```shell
tox -e integration-iomete
```

## Run unit tests
```shell
tox -e unit
```