SHELL := /bin/bash

VENV_BIN ?= python3 -m venv
VENV_DIR ?= .venv
PIP_CMD ?= pip3

USERNAME ?= admin
DB_NAME ?= dms_sample
USERPWD ?= 1Wp2Aide=z=,eLX3RrD4gJ4o54puex
STACK_NAME ?= DMsSampleSetupStack
DB_ENDPOINT ?= mariadb_server
DB_PORT ?= 3306
ENDPOINT_URL = http://localhost.localstack.cloud:4566
export AWS_ACCESS_KEY_ID ?= test
export AWS_SECRET_ACCESS_KEY ?= test
export AWS_DEFAULT_REGION ?= us-east-1

VENV_RUN = . $(VENV_ACTIVATE)

CLOUD_ENV = USERNAME=$(USERNAME) DB_NAME=$(DB_NAME) USERPWD=$(USERPWD) STACK_NAME=$(STACK_NAME)
LOCAL_ENV = USERNAME=$(USERNAME) DB_NAME=$(DB_NAME) USERPWD=$(USERPWD) STACK_NAME=$(STACK_NAME) DB_ENDPOINT=$(DB_ENDPOINT) DB_PORT=$(DB_PORT) ENDPOINT_URL=$(ENDPOINT_URL)

ifeq ($(OS), Windows_NT)
	VENV_ACTIVATE = $(VENV_DIR)/Scripts/activate
else
	VENV_ACTIVATE = $(VENV_DIR)/bin/activate
endif

usage:                    ## Show this help
	@grep -Fh "##" $(MAKEFILE_LIST) | grep -Fv fgrep | sed -e 's/:.*##\s*/##/g' | awk -F'##' '{ printf "%-25s %s\n", $$1, $$2 }'

$(VENV_ACTIVATE):
	test -d $(VENV_DIR) || $(VENV_BIN) $(VENV_DIR)
	$(VENV_RUN); touch $(VENV_ACTIVATE)

venv: $(VENV_ACTIVATE)    ## Create a new (empty) virtual environment

check:					  ## Check if all required prerequisites are available
	@command -v docker > /dev/null 2>&1 || { echo "Docker is not installed."; exit 1; }
	@command -v localstack > /dev/null 2>&1 || { echo "LocalStack is not installed."; exit 1; }
	@command -v python > /dev/null 2>&1 || { echo "Python is not installed."; exit 1; }
	@command -v cdk > /dev/null 2>&1 || { echo "AWS CDK is not installed."; exit 1; }
	@command -v cdklocal > /dev/null 2>&1 || { echo "CDK Local is not installed."; exit 1; }
	@echo "All required prerequisites are available."

start:					  ## Start localstack
	$(LOCAL_ENV) LOCALSTACK_AUTH_TOKEN=$(LOCALSTACK_AUTH_TOKEN) docker compose up --build --detach --wait

install: venv 		 	  ## Install dependencies	
	$(VENV_RUN); $(PIP_CMD) install -r requirements.txt

deploy:					  ## Deploy the stack on LocalStack
	$(VENV_RUN); $(LOCAL_ENV) cdklocal bootstrap --output ./cdk.local.out
	$(VENV_RUN); $(LOCAL_ENV) cdklocal deploy --require-approval never --output ./cdk.local.out

deploy-aws:				 ## Deploy the stack on AWS
	$(VENV_RUN); $(CLOUD_ENV) cdk bootstrap
	$(VENV_RUN); $(CLOUD_ENV) cdk deploy --require-approval never

stop:				     ## Stop LocalStack
	docker-compose down

destroy-aws: venv		 ## Destroy the stack on AWS
	$(VENV_RUN); $(CLOUD_ENV) cdk destroy --require-approval never

run:					 ## Run the application on LocalStack
	$(VENV_RUN); $(LOCAL_ENV) python run.py

run-aws:				 ## Run the application on AWS
	$(VENV_RUN); $(CLOUD_ENV) python run.py

test:					 ## Test the application on LocalStack
	$(VENV_RUN); $(LOCAL_ENV) pytest

logs:					 ## Show logs from LocalStack
	@docker logs localstack-main > logs.txt

.PHONY: usage install start deploy test logs stop deploy-aws test-aws destroy-aws
