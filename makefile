.PHONY: deploy
deploy: render
	sls deploy && sls s3deploy

.PHONY: render_function
render_function:
	@for function in $$(ls serverless/functions/ ); do \
	jinja2 serverless/templates/source/existing_s3_event.yaml serverless/functions/$$function > serverless/templates/rendered/$$function; \
	done

.PHONY: render_functions
render_functions:
	@for function in $$(ls serverless/functions/ ); do \
	jinja2 serverless/templates/source/functions.yaml serverless/functions/$$function >> serverless/templates/rendered/functions.yaml; \
	done

.PHONY: clean_rendered
clean_rendered:
	@rm -rf serverless/templates/rendered
	mkdir -p serverless/templates/rendered
	touch serverless/templates/rendered/functions.yaml

.PHONY: render
render: clean_rendered render_functions render_function