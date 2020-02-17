.AWS_ROLE_NAME ?= oslokommune/iamadmin-SAML

.DEV_ACCOUNT := 988529151833
.PROD_ACCOUNT := 944886649943

.DEV_ROLE := 'arn:aws:iam::$(.DEV_ACCOUNT):role/$(.AWS_ROLE_NAME)'
.PROD_ROLE := 'arn:aws:iam::$(.PROD_ACCOUNT):role/$(.AWS_ROLE_NAME)'

.DEV_PROFILE := saml-origo-dev
.PROD_PROFILE := saml-dataplatform-prod

GLOBAL_PY := python3.7

BUILD_VENV ?= .build_venv
BUILD_PY := $(BUILD_VENV)/bin/python

EXTRA_INDEX:= '--extra-index-url=https://artifacts.oslo.kommune.no/repository/itas-pypip/simple'

.PHONY: init
init: node_modules $(BUILD_VENV)

node_modules: package.json package-lock.json
	npm install

$(BUILD_VENV):
	$(GLOBAL_PY) -m venv $(BUILD_VENV)
	$(BUILD_PY) -m pip install -U pip

.PHONY: format
format: $(BUILD_VENV)/bin/black
	$(BUILD_PY) -m black .

.PHONY: test
test: $(BUILD_VENV)/bin/tox
	$(BUILD_PY) -m tox -p auto -o

.PHONY: upgrade-deps
upgrade-deps: $(BUILD_VENV)/bin/pip-compile
	$(BUILD_VENV)/bin/pip-compile -U ${EXTRA_INDEX}

.PHONY: deploy
deploy: node_modules test login-dev
	@echo "\nDeploying to stage: $${STAGE:-dev}"
	sls deploy --stage $${STAGE:-dev} --aws-profile $(.DEV_PROFILE)

.PHONY: deploy-prod
deploy-prod: node_modules is-git-clean test login-prod
	sls deploy --stage prod --aws-profile $(.PROD_PROFILE)

ifeq ($(MAKECMDGOALS),undeploy)
ifndef STAGE
$(error STAGE is not set)
endif
ifeq ($(STAGE),dev)
$(error Do not undeploy dev)
endif
endif
.PHONY: undeploy
undeploy: login-dev
	@echo "\nUndeploying stage: $(STAGE)\n"
	sls remove --stage $(STAGE) --aws-profile $(.DEV_PROFILE)

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


###
# Python build dependencies
##

$(BUILD_VENV)/bin/pip-compile: $(BUILD_VENV)
	$(BUILD_PY) -m pip install -U pip-tools

$(BUILD_VENV)/bin/pip-sync: $(BUILD_VENV)
	$(BUILD_PY) -m pip install -U pip-tools

$(BUILD_VENV)/bin/tox: $(BUILD_VENV)
	$(BUILD_PY) -m pip install -I virtualenv==16.7.9
	$(BUILD_PY) -m pip install -U tox

$(BUILD_VENV)/bin/%: $(BUILD_VENV)
	$(BUILD_PY) -m pip install -U $*
