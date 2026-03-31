.PHONY: test bench clean

test:
	python -m pytest tests/ -v

bench:
	python benchmarks/run_all.py

clean:
	rm -rf data/ __pycache__ stonedb/__pycache__ tests/__pycache__
	find . -name "*.pyc" -delete
