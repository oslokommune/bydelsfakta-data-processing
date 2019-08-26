bydelsfakta-data-processing
============

API for posting, updating and retrieving metadata.

Feel free to add any fields you'd like/need. We do not currently validate input data, so what you put in,
 you'll get out.


## Setup

1. Install [Serverless Framework](https://serverless.com/framework/docs/getting-started/)
2. Install Serverless plugins: `make init`
3. Install Python toolchain: `python3 -m pip install (--user) tox black pip-tools`
   - If running with `--user` flag, add `$HOME/.local/bin` to `$PATH`

## Test

Install `tox`, (eg. `pip install --user tox`) and then simply run `tox`.

Tox runs the following programs:
 - pytest, picks up both tests written for pytest and the built in unittest framework
 - flake8 linting
 - black automatic formatting


## Function serverless config
- Add a yaml config for the function under `serverless/functions/...yaml`
- Refrence the config file under `serverless/functions.yaml`

## Formatting code

Code is formatted using [black](https://pypi.org/project/black/): `make format`

## Running tests

Tests are run using [tox](https://pypi.org/project/tox/): `make test`

For tests and linting we use [pytest](https://pypi.org/project/pytest/), [flake8](https://pypi.org/project/flake8/) and [black](https://pypi.org/project/black/).

## Deploy

`make deploy` or `make deploy-prod`