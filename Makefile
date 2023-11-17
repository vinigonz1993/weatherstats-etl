setup:
	pip install pipenv
	pipenv shell
	pipenv install
	pipenv install requests

run:
	python run.py