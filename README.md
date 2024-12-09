# evaltools

python tools for evaluating CORDEX datasets

## Installation

Create a new environment, e.g.,
```
conda create -n evaltools python=3.10
```
and install this code using:
```
pip install -e .
```

## Reading in data

evaltools contains some code to easily open and combine datasets, e.g.,

```python
from evaltools.source import get_source_collection, open_and_sort
from dask.distributed import Client

# see https://github.com/euro-cordex/evaltools/issues/5
client = Client(threads_per_worker=1)

catalog = get_source_collection(["tas", "pr"], "mon")
dsets = open_and_sort(catalog, merge=True)
list(dsets.keys())
```
will give a dictionary of combined datasets containing the `tas` and `pr` variables identified by their instance id, e.g.,
```
['CORDEX.EUR-12.CLMcom-Hereon.ERA5.evaluation.r1i1p1f1.ICON-CLM-202407-1-1.v1-r1.v20240920',
 'CORDEX.EUR-12.GERICS.ERA5.evaluation.r1i1p1f1.REMO2020.v1.v20241120',
 'CORDEX.EUR-12.HCLIMcom-SMHI.ERA5.evaluation.r1i1p1f1.HCLIM43-ALADIN.v1-r1.v20241205']
```
