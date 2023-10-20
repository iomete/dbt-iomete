# Testing dbt-iomete


## Set environment variables
```bash
export DBT_IOMETE_HOST=4.236.193.224
export DBT_IOMETE_PORT=80
export DBT_IOMETE_LAKEHOUSE=dbt
export DBT_IOMETE_USER_NAME=admin
export DBT_IOMETE_TOKEN=OzM3t8qCilXaeEWlc7rAMKE2Phex4YJh8NEshziwil4=
```

## Run integration test

```shell
tox -e integration-iomete
```

## Run unit tests
```shell
tox -e unit
```