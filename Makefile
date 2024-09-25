HL = @printf "\033[36m>> $1\033[0m\n"

default: help

.PHONY: help
help:
	@echo "Usage: make <target>\n"
	@grep -E ".+:\s.*?##" $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?##"}; {printf "\033[36m%-24s\033[0m %s\n", $$1, $$2}'

.PHONY: build
build: clean test ## Build package
	$(call HL,build)
	hatch build

.PHONY: clean
clean: ## Clean dist
	$(call HL,clean)
	hatch clean

.PHONY: publish
publish: build ## Publish package
	$(call HL,publish)
	twine upload --repository testpypi dist/*

.PHONY: test
test: ## Run tests
	$(call HL,publish)
	hatch test -v
