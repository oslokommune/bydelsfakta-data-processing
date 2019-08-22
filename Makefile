.AWS_ROLE_NAME ?= oslokommune/iamadmin-SAML

.DEV_ACCOUNT := 988529151833
.PROD_ACCOUNT := 944886649943

.DEV_ROLE := 'arn:aws:iam::$(.DEV_ACCOUNT):role/$(.AWS_ROLE_NAME)'
.PROD_ROLE := 'arn:aws:iam::$(.PROD_ACCOUNT):role/$(.AWS_ROLE_NAME)'

.DEV_PROFILE := saml-origo-dev
.PROD_PROFILE := saml-dataplatform-prod

.PHONY: init
init:
	python3 -m pip install tox black pip-tools
	pip-compile
	npm install

.PHONY: format
format: init
	python3 -m black

.PHONY: deploy
deploy: format test login-dev
	sls deploy --stage dev --aws-profile $(.DEV_PROFILE)
	sls s3deploy --stage dev --aws-profile $(.DEV_PROFILE)

.PHONY: deploy-prod
deploy-prod: format is-git-clean test login-prod
	sls deploy --stage prod --aws-profile $(.PROD_PROFILE) && \
	sls --stage prod downloadDocumentation --outputFileName swagger.yaml

.PHONY: test
test:
	python3 -m tox -p auto

.PHONY: login-dev
login-dev:
	saml2aws login --role=$(.DEV_ROLE) --profile=$(.DEV_PROFILE)

.PHONY: login-prod
login-prod:
	saml2aws login --role=$(.PROD_ROLE) --profile=$(.PROD_PROFILE)

.PHONY: is-git-clean
is-git-clean:
	@status=$$(git fetch origin && git status -s -b) ;\
	if test "$${status}" != "## master...origin/master"; then \
		echo; \
		echo Git working directory is dirty, aborting >&2; \
		false; \
	fi

.PHONY: render_functions
render_functions:
	@for function in $$(ls serverless/functions/ ); do \
	jinja2 serverless/templates/source/functions.yaml -D name=$$function >> serverless/templates/rendered/functions.yaml; \
	done

.PHONY: clean_rendered
clean_rendered:
	@rm -rf serverless/templates/rendered
	mkdir -p serverless/templates/rendered
	touch serverless/templates/rendered/functions.yaml

.PHONY: render
render: clean_rendered render_functions
