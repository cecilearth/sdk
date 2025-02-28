HL = @printf "\033[36m>> $1\033[0m\n"

.PHONY: build
build:
	$(call HL,build)
	@hatch build

.PHONY: clean
clean:
	$(call HL,clean)
	@hatch clean

.PHONY: publish.prod
publish.prod: clean build
	$(call HL,publish.prod)
	@twine upload --repository pypi dist/*

.PHONY: publish.test
publish.test: clean build
	$(call HL,publish.test)
	@twine upload --repository testpypi dist/*

.PHONY: test
test:
	$(call HL,test)
	@hatch test -v
