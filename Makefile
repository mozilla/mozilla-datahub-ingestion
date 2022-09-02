format:
	python3 -m black .
	python3 -m flake8 --max-line-length 100 .

lint:
	python3 -m flake8 --max-line-length 100 .
	python3 -m black --check .
	yamllint .
