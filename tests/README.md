# Testing dbt-iomete


## Set environment variables
```bash
export DBT_IOMETE_ACCOUNT_NUMBER=707505543825
export DBT_IOMETE_HOST_NAME=eu-central-1.iomete.com
export DBT_IOMETE_LAKEHOUSE=virtual-warehouse-1
export DBT_IOMETE_USER_NAME=<username>
export DBT_IOMETE_PASSWORD=<password>
```

## Run integration test

```shell
tox -e integration-iomete
```

## Run unit tests
```shell
tox -e unit
```