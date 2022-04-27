# restful-grids
Exploring modern RESTful services for gridded data

## Resources
Use this S3 bucket for test data: s3://ioos-code-sprint-2022  
Several zarr datasets have been added to it. It also contains a static STAC catalog for identifying the data.

## Setup
[Miniconda](https://docs.conda.io/en/latest/miniconda.html) is recommended to manage Python dependencies.  

In the Anaconda prompt, you can load the `environment.yml` file to configure your environment:
`conda env create -f environment.yml`

Once you install the environment, you will need to activate it using

`conda activate code-sprint-2022`

To update your conda environment with any new packages added or removed to the `environment.yml` file use

`conda env update -f environment.yml --prune`

Alternatively, you can install dependencies with `pip` and `virtualenv`: 

```bash
virutalenv env/
source env/bin/activate
pip install -r requirements.txt
```