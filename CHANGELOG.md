## dbt-iomete 1.7.9 (May 8, 2025)
- Added support for table properties via `tblproperties` config.
- Added support for predicates in incremental models via `incremental_predicates` config.
- Fixed an issue with the merge and append strategies where columns removed from the model were not handled correctly.

## dbt-iomete 1.7.8 (May 6, 2025)
- Fixed dbt-common lib issue

## dbt-iomete 1.7.7 (May 5, 2025)
- Add explicit multi catalog support and fix snapshot apart from default catalog
- Add multiple unique key support for incremental tables

## dbt-iomete 1.7.6 (May 1, 2025)
- Fixed incremental models with on_schema_change set as sync_all_columns

## dbt-iomete 1.7.5 (Mar 10, 2025)
- Fixed type not getting marked as `view` for relations that are views
- Fixed model execution marked as OK even if the model fails

## dbt-iomete 1.7.4 (Jan 20, 2025)
- Added Domain and multi catalog support
- Fixed Incremental table creation flow
- Fixed test cases
- Added Python support till v3.11

## dbt-iomete 1.7.3 (Oct 10, 2024)
- Added Data Plane parameter for profile

## dbt-iomete 1.7.2 (Nov 24, 2023)
- Minor fixes in incremental executions.

## dbt-iomete 1.7.1 (Nov 14, 2023)
- `dbt` version upgraded to 1.7.0
- Added support to python models

## dbt-iomete 1.7.0 (Oct 20, 2023)
- Upgraded `py-hive-iomete` library to version 1.3.0
- Removed `workspace_id`, additional parameters like scheme, port is supported now

## dbt-iomete 1.5.0 (Feb 13, 2023)
- Upgraded `py-hive-iomete` library to version 1.1.0
- Replaced `account_number` config with `workspace_id`
- Remove password support, personal access tokens should be used instead

## dbt-iomete 1.4.0 (Nov 13, 2022)
- Multiple `describe table` calls are replaced by a single API call. In the DBT run, you will see a big improvement in the completion time. Especially if a database contains many  tables.

## dbt-iomete 1.2.0 (Nov 13, 2022)
- Full schema sync works with Iceberg tables as well

## dbt-iomete 1.1.0 (Nov 7, 2022)
Update iomete connection parameters and improve error messaging

- Improve error messaging
- Update profile template to reflect the new iomete parameters
- moved to py-hive-iomete from py-hive
- changed cluster parameter to lakehouse

## dbt-iomete 1.0.2 (Sep 20, 2022)
- `profile_template.yml` Updated according to previous release 1.0.1

## dbt-iomete 1.0.1 (Sep 18, 2022)
- Added new connection parameter `account_number`, based on new format of Lakehouse JDBC connection url.
To enable the Lakehouse connection, you need to add the account_number parameter to the ~/.dbt/profiles.yml file.
Follow profile [documentation](https://docs.iomete.com/docs/profile-setup)

## dbt-iomete 1.0.0 (May 25, 2022)
- dbt-iomete adapter implemented and released ([here](https://github.com/iomete/dbt-iomete/commit/e692dfb8b59ff699ee546ecef7ae46388e2352f5))