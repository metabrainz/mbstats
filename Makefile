init:
	pip install -r requirements.txt

initdev:
	pip install -r requirements-dev.txt

check: initdev
	isort -c -rc mbstats/
	pyflakes mbstats/
	pylint mbstats/
	
test:
	python -m unittest discover mbstats/tests/

.PHONY: init test
