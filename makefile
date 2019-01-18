.PHONY: deploy
deploy:
	sls deploy && sls s3deploy