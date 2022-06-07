from naas_drivers import snowflake
import os

# TODO: Create other features' tests as soon as SF test account is available


def test_snowflake_connection():
    snowflake.connect(
        account=os.environ['SNOWFLAKE_ACCOUNT'],
        username=os.environ['SNOWFLAKE_USER'],
        password=os.environ['SNOWFLAKE_PASSWORD']
    )

    assert snowflake.connected
