from naas_drivers import snowflake
import pytest

# TODO: Create other features' tests as soon as SF test account is available


@pytest.fixture(scope='session')
def sf_credentials_fake():
    return {
        'username': 'test_username',
        'password': 'invalid_p@ssw0rd!',
        'account': 'abc1010.eu-north-999.aws'
    }


def test_snowflake_fake_connection(sf_credentials_fake):
    with pytest.raises(Exception):
        snowflake.connect(**sf_credentials_fake)
