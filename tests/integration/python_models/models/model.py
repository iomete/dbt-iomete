def model(dbt, session):
    dbt.config(
        materialized="table",
        spark_job_id="dbt-runner",
        spark_job_overrides={}
    )

    df = session.createDataFrame(
        [
            ("Buenos Aires", "Argentina", -34.58, -58.66),
            ("Brasilia", "Brazil", -15.78, -47.91),
            ("Santiago", "Chile", -33.45, -70.66),
            ("Bogota", "Colombia", 4.60, -74.08),
            ("Caracas", "Venezuela", 10.48, -66.86),
        ],
        ["City", "Country", "Latitude", "Longitude"]
    )

    return df
