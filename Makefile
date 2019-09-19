#---------------------------------------------------------------------------------------#
#                                                                                       #
#	 - Makefile, version 1.8.2                                                          #
#	 - PROJECT:  spotlib                                                                #
# 	 - copyright, Blake Huber.  All rights reserved.                                    #
#                                                                                       #
#---------------------------------------------------------------------------------------#


PROJECT := lambda-spotprices
CUR_DIR = $(shell pwd)
PYTHON_VERSION := python3
PYTHON3_PATH := $(shell which $(PYTHON_VERSION))
GIT := $(shell which git)
VENV_DIR := $(CUR_DIR)/p3_venv
PIP_CALL := $(VENV_DIR)/bin/pip
PANDOC_CALL := $(shell which pandoc)
ACTIVATE = $(shell . $(VENV_DIR)/bin/activate)
MAKE = $(shell which make)
MODULE_PATH := $(CUR_DIR)/Code
SCRIPTS := $(CUR_DIR)/scripts
DOC_PATH := $(CUR_DIR)/docs
REQUIREMENT = $(CUR_DIR)/requirements.txt
VERSION_FILE = $(CUR_DIR)/$(PROJECT)/_version.py

S3_BASEPATH := s3-$(AWS_REGION)-mpc-install-$(TARGET_ENV)
ZIPNAME = $(PROJECT)-codebase.zip
S3KEY = Code/$(PROJECT)
CFN_TEMPLATE = $(PROJECT).template.yml


# --- rollup targets  ------------------------------------------------------------------------------


.PHONY: fresh-install fresh-test-install deploy-test deploy-prod

zero-source-install: clean source-install   ## Install (source: local). Zero prebuild artifacts

zero-test-install: clean setup-venv test-install  ## Install (source: testpypi). Zero prebuild artifacts

deploy-test: clean testpypi  ## Deploy (testpypi), generate all prebuild artifacts

deploy-prod: clean pypi   ## Deploy (pypi), generate all prebuild artifacts


# --- targets -------------------------------------------------------------------------------------


.PHONY: pre-build
pre-build:    ## Remove residual build artifacts
	rm -rf $(CUR_DIR)/dist
	mkdir $(CUR_DIR)/dist


setup-venv: $(VENV_DIR)

$(VENV_DIR): clean pre-build  ## Create and activiate python virtual package environment
	$(PYTHON3_PATH) -m venv $(VENV_DIR)
	. $(VENV_DIR)/bin/activate && $(PIP_CALL) install -U setuptools pip && \
	$(PIP_CALL) install -r $(REQUIREMENT)


.PHONY: test
test: setup-venv  ## Run pytest unittests. Optional Param: PDB, MODULE
	if [ $(MODULE) ]; then \
	bash $(CUR_DIR)/scripts/make-test.sh --package-path $(MODULE_PATH) --module $(MODULE); \
	else bash $(CUR_DIR)/scripts/make-test.sh  --package-path $(MODULE_PATH); fi


.PHONY: test-coverage
test-coverage:  setup-venv  ## Run pytest unittests; generate coverage report
	bash $(CUR_DIR)/scripts/make-test.sh  --package-path $(MODULE_PATH) --coverage


.PHONY: test-complexity
test-complexity:  setup-venv  ## Run pytest unittests; generate McCabe Complexity Report
	bash $(CUR_DIR)/scripts/make-test.sh  --package-path $(MODULE_PATH) --complexity


.PHONY: test-pdb
test-pdb:  setup-venv  ## Run pytest unittests with debugging output on
	bash $(CUR_DIR)/scripts/make-test.sh  --package-path $(MODULE_PATH) --pdb


.PHONY: test-help
test-help:   ## Print runtime options for running pytest unittests
	bash $(CUR_DIR)/scripts/make-test.sh  --help


build: setup-venv   ## Build zip file for upload to Amazon S3
	cd $(CUR_DIR)/Code && zip -r $(CUR_DIR)/dist/$(ZIPNAME) *.py connectors core
	cd $(VENV_DIR)/lib/*/site-packages && zip -ur $(CUR_DIR)/dist/$(ZIPNAME) *
	cd $(CUR_DIR)/cloudformation && cp * $(CUR_DIR)/dist/
	sed -i -e 's/\__MPCBUILDVERSION__/$(MPCBUILDVERSION)/' $(CUR_DIR)/dist/$(CFN_TEMPLATE)


validate:   ##  Use awscli to validate cloudformation template syntax
	@echo "CloudFormation validation"
	aws cloudformation validate-template --region $(AWS_REGION) --template-body file://$(CUR_DIR)/cloudformation/$(CFN_TEMPLATE)


deploy:  ## Deploy lambda zip archive to Amazon S3
	$(eval S3_BUCKET_PREFIX = s3-$(AWS_REGION)-mpc-install)
	aws s3 cp $(CUR_DIR)/dist/$(ZIPNAME) s3://$(S3_BUCKET_PREFIX)-$(TARGET_ENV)/$(S3KEY)/$(ZIPNAME)
	aws s3 cp $(CUR_DIR)/dist/$(CFN_TEMPLATE) s3://$(S3_BUCKET_PREFIX)-$(TARGET_ENV)/CFT/$(CFN_TEMPLATE)


.PHONY: help
help:   ## Print help index
	@printf "\n\033[0m %-15s\033[0m %-13s\u001b[37;1m%-15s\u001b[0m\n\n" " " "make targets: " $(PROJECT)
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {sub("\\\\n",sprintf("\n%22c"," "), $$2);printf "\033[0m%-2s\033[36m%-20s\033[33m %-8s\033[0m%-5s\n\n"," ", $$1, "-->", $$2}' $(MAKEFILE_LIST)
	@printf "\u001b[37;0m%-2s\u001b[37;0m%-2s\n\n" " " "___________________________________________________________________"
	@printf "\u001b[37;1m%-3s\u001b[37;1m%-3s\033[0m %-6s\u001b[44;1m%-9s\u001b[37;0m%-15s\n\n" " " "  make" "deploy-[test|prod] " "VERSION=X" " to deploy specific version"


.PHONY: clean
clean:   ## Remove generic build artifacts common to most targets
	@echo "Clean project directories"
	rm -rf $(VENV_DIR) || true
	rm -rf $(CUR_DIR)/dist || true
	rm -rf $(CUR_DIR)/*.egg* || true
	rm -f $(CUR_DIR)/README.rst || true
	rm -rf $(CUR_DIR)/$(PROJECT)/__pycache__ || true
	rm -rf $(CUR_DIR)/$(PROJECT)/core/__pycache__ || true
	rm -rf $(CUR_DIR)/tests/__pycache__ || true
	rm -rf $(CUR_DIR)/docs/__pycache__ || true
	rm -rf $(CUR_DIR)/.pytest_cache || true
	rm -rf $(CUR_DIR)/build || true
