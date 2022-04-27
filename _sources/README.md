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

## Running This Work-In-Progress

Once you install your environment, you can run your local server with the:
- Wave Watch 3 (ww3) dataset, which can be downloaded [here]()
- Global Forecast System (GFS) in Zarr format hosted on the cloud

Once you have your data, use following steps:

### Start the Server
You can start the server using the `main.py` in the `/xpublish` directory

```
cd xpublish
python main.py
```

This will spin up a server, accessible using the following link (localhost:9005):

```
INFO:     Uvicorn running on http://0.0.0.0:9005 (Press CTRL+C to quit)
INFO:     Started reloader process [5152] using statreload
INFO:     Started server process [5155]
INFO:     Waiting for application startup.
INFO:     Application startup complete.
```

When you go to the web address, you you will see a page specifying which datasets are available

```
["ww3","gfs"]
```

We can look at the GFS dataset, by adding `/datasets/gfs` to the url, which results in a web-rendered version of the dataset

![GFS-web](images/gfs-web.png)

