![Bump version](https://github.com/jupyter-naas/drivers/workflows/Bump%20version/badge.svg)
![GitHub license](https://img.shields.io/github/license/jupyter-naas/drivers)
![Test Python package](https://github.com/jupyter-naas/drivers/workflows/Test%20Python%20package/badge.svg)
![Upload Python Package](https://github.com/jupyter-naas/drivers/workflows/Upload%20Python%20Package/badge.svg)
![codecov](https://codecov.io/gh/jupyter-naas/drivers/branch/master/graph/badge.svg?token=IUF0AKYEB0)
![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=jupyter-naas_drivers&metric=sqale_rating)](https://sonarcloud.io/dashboard?id=jupyter-naas_naas)
<a href="#badge">
  <img alt="semantic-release" src="https://img.shields.io/badge/%20%20%F0%9F%93%A6%F0%9F%9A%80-semantic--release-e10079.svg">
</a>
<a href="http://commitizen.github.io/cz-cli/"><img alt="Commitizen friendly" src="https://img.shields.io/badge/commitizen-friendly-brightgreen.svg"></a>
![PyPI](https://img.shields.io/pypi/v/naas_drivers)

# Welcome to Naas Drivers

🏎 Simple formulas to build highly efficient scripts

## Why Naas Drivers?

We came from excel with the conviction that Python is awesome .
Python can be use by developpers, or in low-code way.

<br/>
That what we try to achieve with naas_drivers.
Simple formula to interact with powerfull tools.
<br/>
Each driver try to return dataframe to strandardise the output.

##install

`python -m pip install naas_drivers`

Few drivers need specific env var set, that will be notified in the documentation:

<p>
  <a href="https://naas.gitbook.io/drivers/" title="Redirect to Documentation">
    <img width="200px" src="https://raw.githubusercontent.com/jupyter-naas/drivers/main/images/gitbook.svg" alt="Gitbooks drivers" />
  </a>
 </p>

# Dev

## Install 

`python -m pip install -e .`

## Check lint

`python3 -m black naas` format better
`python3 -m flake8 naas` check if any left error

## Publish

this auto publish on pypip by github action on main branch

### Authors:
* [Martin donadieu](https://github.com/riderx)
