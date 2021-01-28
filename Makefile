init:
	pip install -r requirements.txt

initdev:
	pip install -r requirements-dev.txt

check: initdev
	-isort --check-only --diff mbstats/
	-pyflakes mbstats/
	-pylint mbstats/
	
test:
	python -m unittest discover -v mbstats/tests/

coverage:
	coverage run -m unittest discover mbstats/tests/

dockerbuild:
	docker build -t mbstats -f Dockerfile .

.PHONY: init test
