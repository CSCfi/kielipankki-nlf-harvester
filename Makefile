
constraints.txt:
	# NB: we extract the Python major+minor version currently in use with
	# python --version | grep -Eo '[0-9].[0-9]'
	curl https://raw.githubusercontent.com/apache/airflow/constraints-2.4.0/constraints-3.8.txt --output constraints.txt
	# Remove the line that pins pytest version to more than two years old one to
	# allow us to specify a newer one ourselves
	sed -i "/pytest==/d" constraints.txt

requirements: constraints.txt
	pip install -r requirements_dev.txt -c constraints.txt
