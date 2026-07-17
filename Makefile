all:
	@echo nothing special
clean:
	rm -rf dist/*
sdist:
	python3 -m pipx run build --sdist
release:
	twine upload dist/redisk-*.tar.gz -r pypi
