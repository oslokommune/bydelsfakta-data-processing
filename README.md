# bydelsfakta-data-processing
Python functions that are used to process SSB datasets


## Creating new functions
Add a new config file to `serverless/functions` with the name of the function. 
The `dataset` array is used to create s3 notification requests, which will trigger the function
when an object is uploaded. 


`make render` will render the correct serverless config, `sls deploy` and `sls s3deploy`
will deploy the application to aws. For your convenience you can use `make deploy`.

## Test

Install `tox`, (eg. `pip install --user tox`) and then simply run `tox`.

Tox runs the following programs:
 - pytest, picks up both tests written for pytest and the built in unittest framework
 - flake8 linting
 - black automatic formatting
