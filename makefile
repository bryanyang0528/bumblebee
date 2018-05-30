
pip:
	@echo "install requirements"
	pip install attrs target=.

build: pip
	@echo "Build package"
	rm -rf dist
	mkdir dist && zip -r dist/bumblebee.zip .
