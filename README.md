# Drivers
🏎simple formulas to build highly efficient scripts

## check lint

`python3 -m black naas` format better
`python3 -m flake8 naas` check if any left error

## publish

bump version
`cz bump --changelog`
create release
`python3 setup.py sdist`
upload release
`python3 -m twine upload dist/* -u YOUR_USERNAME`
