from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """
    Runs data quality checks.

    Expects `tests` as a list of dicts like:
    {
      "check_sql": "SELECT COUNT(*) FROM users WHERE userid IS NULL;",
      "expected_result": 0,
      "comparison": "=="
    }

    Supported comparisons: "==", "!=", ">", ">=", "<", "<="
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 tests=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)

        if tests is None or not isinstance(tests, list) or len(tests) == 0:
            raise ValueError("DataQualityOperator requires a non-empty list of 'tests'")

        self.redshift_conn_id = redshift_conn_id
        self.tests = tests

    def _compare(self, actual, expected, comparison):
        if comparison == "==":
            return actual == expected
        if comparison == "!=":
            return actual != expected
        if comparison == ">":
            return actual > expected
        if comparison == ">=":
            return actual >= expected
        if comparison == "<":
            return actual < expected
        if comparison == "<=":
            return actual <= expected
        raise ValueError(f"Unsupported comparison operator: {comparison}")

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Starting data quality checks (%d tests)", len(self.tests))

        for i, test in enumerate(self.tests, start=1):
            check_sql = test.get("check_sql")
            expected = test.get("expected_result")
            comparison = test.get("comparison", "==")

            if not check_sql:
                raise ValueError(f"Test #{i} is missing 'check_sql'")
            if expected is None:
                raise ValueError(f"Test #{i} is missing 'expected_result'")

            self.log.info("Running test #%d: %s", i, check_sql)

            records = redshift.get_records(check_sql)
            if not records or not records[0] or records[0][0] is None:
                raise ValueError(f"Test #{i} returned no results: {check_sql}")

            actual = records[0][0]
            passed = self._compare(actual, expected, comparison)

            if not passed:
                raise ValueError(
                    f"Data quality check failed (test #{i}). "
                    f"Actual={actual}, Expected {comparison} {expected}. SQL: {check_sql}"
                )

            self.log.info("Test #%d passed. Actual=%s, Expected %s %s", i, actual, comparison, expected)

        self.log.info("All data quality checks passed ✅")
