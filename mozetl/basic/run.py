from datetime import date, timedelta
from moztelemetry.dataset import Dataset


def generic_job(sc, sqlContext, source_doc_type, transform_function, s3_path,
                submission_date=None, save=True):
    if submission_date is None:
        submission_date = (date.today() - timedelta(1)).strftime("%Y%m%d")

    pings = Dataset.from_source(
        "telemetry"
    ).where(
        docType=source_doc_type,
        submissionDate=submission_date,
        appName="Firefox",
    ).records(sc)

    transformed_pings = transform_func(sqlContext, pings)

    if save:
        # path = 's3://telemetry-parquet/testpilot/txp_pulse/v1/submission_date={}'
        path = s3_path + '/submission_date={}'.format(submission_date)
        transformed_pings.repartition(1).write.mode('overwrite').parquet(path)

    return transformed_pings
