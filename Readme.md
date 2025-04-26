## Creating virtual environment using python - 
1 - Navigate to your project directory

2 - Create a virtual environment named '.venv'
	python -m venv <env-name>

3 - Activate the virtual environment
	source <env-name>/bin/activate

4 - Deactivate the virtual environment
	deactivate

## Creating virtual environment using conda - 
conda -V
conda create --name myenv python=x.x
conda activate myenv
conda install numpy pandas matplotlib
conda deactivate / source deactivate
conda remove --name ENV_NAME --all

## you can create virtual environment using any one of above and then install/uninstall required libraries
pip install snowflake-connector-python
pip uninstall snowflake-connector-python
pip install snowflake-connector-python==2.8.3
