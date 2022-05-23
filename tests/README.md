# Testing dbt-iomete


## Set environment variables
```bash
export DBT_IOMETE_HOST_NAME=iomete_host_name
export DBT_IOMETE_CLUSTER_NAME=iomete_cluster_name
export DBT_IOMETE_USER_NAME=iomete_user_name
export DBT_IOMETE_PASSWORD=password
```

## Run sample dbt

```shell

source .toxrunner/bin/activate

# install globally
pip3 install .

cd sample-test/test1
jdbc:hive2://dwh-910848238944.iomete.com/;transportMode=http;ssl=true;httpPath=reporting/cliservice
```

## Run integration test

```shell
tox -e integration-iomete
```

## Run unit tests
```shell
tox -e unit
```