# Drivers
🏎 Simple formulas to build highly efficient scripts

## Check lint

`python3 -m black naas` format better
`python3 -m flake8 naas` check if any left error

## Publish

Allow easy deploy by setting password in keyring
`python3 -m keyring set https://upload.pypi.org/legacy/ bobapp`

Then publish
`publish.sh`