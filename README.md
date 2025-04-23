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

cat = get_source_collection(["tas", "pr"], frequency="mon", add_fx=True)
dsets = open_and_sort(cat, merge_fx=True, apply_fixes=True)

list(dsets.keys())
```
will give a dictionary of combined datasets containing the `tas` and `pr` variables identified by their instance id, e.g.,
```
['CORDEX-CMIP6.EUR-12.GERICS.ERA5.evaluation.r1i1p1f1.REMO2020-2-2.v1-r1.mon.v20241120',
 'CORDEX-CMIP6.EUR-12.CLMcom-CMCC.ERA5.evaluation.r1i1p1f1.CCLM6-0-1-URB.v1-r1.mon.v20250201',
 'CORDEX-CMIP6.EUR-12.RMIB-UGent.ERA5.evaluation.r1i1p1f1.ALARO1-SFX.v1-r1.mon.v20241009',
 'CORDEX-CMIP6.EUR-12.CLMcom-Hereon.ERA5.evaluation.r1i1p1f1.ICON-CLM-202407-1-1.v1-r1.mon.v20240920',
 'CORDEX-CMIP6.EUR-12.HCLIMcom-SMHI.ERA5.evaluation.r1i1p1f1.HCLIM43-ALADIN.v1-r1.mon.v20241205',
 'CORDEX-CMIP6.EUR-12.KNMI.ERA5.evaluation.r1i1p1f1.RACMO23E.v1-r1.mon.v20241216',
 'CORDEX-CMIP6.EUR-12.GERICS.ERA5.evaluation.r1i1p1f1.REMO2020-2-2-MR2.v1-r1.mon.v20241120']
```
`open_and_sort` will also throw out inconsistent datasets if it can not be fixed, e.g., wrong grid mapping defintions, etc. Some of the issues are logged.
