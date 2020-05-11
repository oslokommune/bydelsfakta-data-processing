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

## Run a function locally

Start by activating a virtual environment

```python3
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

The handler in each function needs an event with information on what types of datasets and outputs it requires.

An event looks like this:

```
{
    "execution_name": "name-of-execution",
    "task": "lambda_invoker",
    "payload": {
        "pipeline": {
            "id": "id-of-the-pipeline",
            "task_config": {
            "write_to_processed": {
                "output_stage": "processed"
            },
            "lambda_invoker": {
                "arn": "bydelsfakta-data-processing-<name-of-lambda-function>",
                "type": "type"
            }
            }
        },
        "output_dataset": {
            "id": "id-of-output-dataset",
            "version": "1",
            "edition": "ISODATE",
            "s3_prefix": "%stage%/yellow/bydelsfakta-grafdata/<name-of-dataset>/version=1/edition=<ISODATE>/"
        },
        "step_data": {
            "s3_input_prefixes": { // as many dataset-ids the function needs
                "dataset-id": "s3-path",
            },
            "status": "PENDING",
            "errors": []
        }
    }
}
```

Example event:

```
{
    "execution_name": "testtest",
    "task": "lambda_invoker",
    "payload": {
        "pipeline": {
            "id": "befolkningsutvikling-og-forventet-utvikling",
            "task_config": {
            "write_to_processed": {
                "output_stage": "processed"
            },
            "lambda_invoker": {
                "arn": "bydelsfakta-data-processing-befolkningsutvkl-forv-utvkl",
                "type": "historisk"
            }
            }
        },
        "output_dataset": {
            "id": "befolkningsutvikling-og-forventet-utvikling",
            "version": "1",
            "edition": "20200420T201739",
            "s3_prefix": "%stage%/yellow/bydelsfakta-grafdata/befolkningsutvikling-og-forventet-utvikling/version=1/edition=20200420T201739/"
        },
        "step_data": {
            "s3_input_prefixes": {
                "befolkning-etter-kjonn-og-alder": "processed/yellow/befolkning-etter-kjonn-og-alder/version=1/edition=20200330T173636/",
                "befolkningsframskrivninger": "processed/green/befolkningsframskrivninger/version=1/edition=20200323T131119/",
                "dode": "processed/green/dode/version=1/edition=20200406T195244/",
                "flytting-fra-etter-alder": "processed/green/flytting-fra-etter-alder/version=1/edition=20200207T111126/",
                "flytting-til-etter-alder": "processed/green/flytting-til-etter-alder/version=1/edition=20200207T125551/",
                "fodte": "processed/green/fodte/version=1/edition=20200420T201652/"
            },
            "status": "PENDING",
            "errors": []
        }
    }
}
```

Go inside python and run the command with the event as an argument:

```python3
python

event = { // The whole event-object here // }
from functions.<function-name> import start

start(event, None)
```

You may need to disable the AWS SDK XRAY function - set an env variable: `AWS_XRAY_SDK_ENABLED = false`

If you want to run  it with debug-mode in pycharm (or some other IDE):

```python3
if __name__ == '__main__':
    event = { // event-object // }
    
    start(event, None)
```