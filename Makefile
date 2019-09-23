init:
	pip install -r requirements.txt

initdev:
	pip install -r requirements-dev.txt

check: initdev
	-isort -c -rc -df mbstats/
	-pyflakes mbstats/
	-pylint mbstats/
	
test:
	python -m unittest discover mbstats/tests/

coverage:
	coverage run -m unittest discover mbstats/tests/

dockerbuild:
	docker build -t mbstats -f docker/Dockerfile .

.PHONY: init test
