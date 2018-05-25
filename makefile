
build:
	@echo "Build package"
	rm -rf dist
	mkdir dist && zip -r dist/pysparka.zip .
