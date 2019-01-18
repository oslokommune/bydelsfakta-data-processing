.PHONY: render deploy
deploy:
	sls deploy && sls s3deploy

.PHONY: render_function
render_function:
	@for function in $$(ls serverless/functions/ ); do \
	jinja2 serverless/templates/source/existing_s3_event.yaml serverless/functions/$$function > serverless/templates/rendered/$$function; \
	done

.PHONY: render_functions
render_functions: clean_functions
	@for function in $$(ls serverless/functions/ ); do \
	jinja2 serverless/templates/source/functions.yaml serverless/functions/$$function >> serverless/templates/rendered/functions.yaml; \
	done

.PHONY: clean_functions
clean_functions:
	@rm -f serverless/templates/rendered/functions.yaml

.PHONY: render
render: render_functions render_function