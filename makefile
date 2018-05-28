
pip:
	@echo "install requirements"
	pip install -r requirements.txt

build: pip
	@echo "Build package"
	rm -rf dist
	mkdir dist && zip -r dist/bumblebee.zip .
