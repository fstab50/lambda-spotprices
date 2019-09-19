PROJECT := serverless-monitor
PYTHON_VERSION := python3.6
CUR_DIR = $(shell pwd)
VENV_DIR := $(CUR_DIR)/venv
PIP_CALL := $(VENV_DIR)/bin/pip

PYTHON3_PATH := $(shell which $(PYTHON_VERSION))

AWS_REGION :=
TARGET_ENV :=

S3_BASEPATH := s3-$(AWS_REGION)-mpc-install-$(TARGET_ENV)
MODULE_PATH := $(CUR_DIR)/Code
DOC_PATH := $(CUR_DIR)/doc
REQUIREMENT = $(CUR_DIR)/requirements.txt
ZIPNAME = $(PROJECT)-$(MPCBUILDVERSION).zip
S3KEY = Code/$(PROJECT)
CFN_TEMPLATE = $(PROJECT).template.yml

all: deploy


pre-build:
	rm -rf $(CUR_DIR)/dist
	mkdir $(CUR_DIR)/dist


.PHONY: setup-venv
setup-venv: pre-build
	$(PYTHON3_PATH) -m venv $(VENV_DIR)
	. $(VENV_DIR)/bin/activate && $(PIP_CALL) install -U setuptools pip && \
	$(PIP_CALL) install -r $(CUR_DIR)/requirements.txt || true


.PHONY: test
test: setup-venv
	@echo "Not yet implemented"
	#if [ $(JOB_NAME) ]; then coverage run --source Code -m py.test $(CUR_DIR)/tests; coverage html; else \
	#bash $(CUR_DIR)/scripts/make-test.sh $(CUR_DIR) $(VENV_DIR) $(MODULE_PATH) $(PDB); fi


build: setup-venv
	cd $(CUR_DIR)/Code && zip -r $(CUR_DIR)/dist/$(ZIPNAME) *.py connectors core
	cd $(VENV_DIR)/lib/*/site-packages && zip -ur $(CUR_DIR)/dist/$(ZIPNAME) *
	cd $(CUR_DIR)/cloudformation && cp * $(CUR_DIR)/dist/
	sed -i -e 's/\__MPCBUILDVERSION__/$(MPCBUILDVERSION)/' $(CUR_DIR)/dist/$(CFN_TEMPLATE)


validate:
	@echo "CloudFormation validation"
	aws cloudformation validate-template --region $(AWS_REGION) --template-body file://$(CUR_DIR)/cloudformation/$(CFN_TEMPLATE)


deploy:
	$(eval S3_BUCKET_PREFIX = s3-$(AWS_REGION)-mpc-install)
	aws s3 cp $(CUR_DIR)/dist/$(ZIPNAME) s3://$(S3_BUCKET_PREFIX)-$(TARGET_ENV)/$(S3KEY)/$(ZIPNAME)
	aws s3 cp $(CUR_DIR)/dist/$(CFN_TEMPLATE) s3://$(S3_BUCKET_PREFIX)-$(TARGET_ENV)/CFT/$(CFN_TEMPLATE)


clean:
	rm -rf $(CUR_DIR)/dist
	rm -rf $(CUR_DIR)/venv
	rm -rf $(CUR_DIR)/Code/__pycache__ || true
	rm -rf $(CUR_DIR)/tests/__pycache__ || true
	rm -rf $(CUR_DIR)/.pytest_cache || true
