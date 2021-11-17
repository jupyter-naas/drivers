from naas_drivers.tools.taggun import Taggun
from pprint import pprint
import os


def test_taggun():
    '''
    Test the taggun drive.

    right now, this test passes no matter what, and prints the results from taggun
    to the consol. Basically it requires a manual check of the returned data from the
    sample receipt.

    REQUIRES: taggun api key in env var TAGGUN_API

    TODO: mock the network requests and feed back known data. this way there is no need
    for an api key, and it is easier to validate the mechanics of this driver.
    '''
    TAGGUN_KEY = os.getenv('TAGGUN_API')
    if not TAGGUN_KEY:
        print('No TAGGUN_API env var missing, returning...')
        return

    tg = Taggun()
    tg.connect(
        TAGGUN_KEY, "tests/taggun/sample receipt.jpg"
    )
    data = tg.send()
    pprint(data)
    assert True


if __name__ == "__main__":
    test_taggun()
