<p align="center">
  <img src="docs/iomete-logo.png" alt="iomete logo" width="300" /> <img src="https://raw.githubusercontent.com/dbt-labs/dbt/ec7dee39f793aa4f7dd3dae37282cc87664813e4/etc/dbt-logo-full.svg" alt="dbt logo" width="250"/>
</p>
<p align="center">
  <a href="https://github.com/iomete/dbt-iomete/actions/workflows/main.yml">
    <img src="https://github.com/iomete/dbt-iomete/actions/workflows/main.yml/badge.svg?event=push" alt="Unit Tests Badge"/>
  </a>
  <a href="https://github.com/iomete/dbt-iomete/actions/workflows/release.yml">
    <img src="https://github.com/iomete/dbt-iomete/actions/workflows/release.yml/badge.svg?event=push" alt="Release Badge"/>
  </a>
</p>

**[dbt](https://www.getdbt.com/)** enables data analysts and engineers to transform their data using the same practices that software engineers use to build applications.

dbt is the T in ELT. Organize, cleanse, denormalize, filter, rename, and pre-aggregate the raw data in your warehouse so that it's ready for analysis.

## dbt-iomete

The `dbt-iomete` package contains all the code enabling dbt to work with iomete.

This adapter is forked from the [dbt-spark](https://github.com/dbt-labs/dbt-spark)

## Getting started

### Installation

```shell
pip install dbt-iomete
```

Alternatively, you can install the package from GitHub with:

```shell
pip install git+https://github.com/iomete/dbt-iomete.git
```

### Profile Setup

```yaml
iomete:
  target: dev
  outputs:
    dev:
      type: iomete
      cluster: cluster_name
      host: <region_name>.iomete.com
      account_number: iomete_account_number
      port: 443
      schema: database_name
      user: iomete_user_name
      password: iomete_user_password
```

For more information, consult [the docs](https://docs.iomete.com/docs/profile-setup).