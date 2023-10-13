[![Test Package](https://github.com/grisslab/grein_loader/actions/workflows/test.yaml/badge.svg?branch=main)](https://github.com/grisslab/grein_loader/actions/workflows/test.yaml)
[![PyPI version](https://badge.fury.io/py/grein-loader.svg)](https://badge.fury.io/py/grein-loader)

# grein_loader

Python package to automatically download datasets from GREIN

http://www.ilincs.org/apps/grein

#### Introduction
Grein Loader enables users to access data from the GREIN website by using the GSE identification number

#### Installation

Install the package from pypi by using: 
```
pip install grein_loader
```


### Usage

The package allows you to download the description, metadata and the raw counts of a GREIN dataset based on the GSE id. 
The datasets from GREIN are publicly available and can be accessed via the [GREIN webpage](http://www.ilincs.org/apps/grein/?gse=). 
Each dataset uses an GEO accession id which allows you to access its data.

#### load_dataset()
```
geo_accession = "GSE112749"
description, metadata, count_matrix = grein_loader.load_dataset(geo_accession)
```

Input/Output parameters
```
Input parameter:
| gse_id | string | GEO accession id

Output parameter: 
| description  | dictionary      | description of dataset
| metadata     | dictionary      | metadata of dataset
| count_matrix | pandas dataframe| numpy array of raw counts
```

#### load_overview()
loads a number of datasets from Grein, the datasets are also listed on the main paige of GREIN
```
number_of_datasets = 10
overview = loader.load_overview(number_of_datasets)
```

The function returns a list of dictionaries, each dictionary contains the GSE id, number of samples, species and description 
provided from GREIN. 

```
Input parameter:
number_of_samples

Output parameter: 
list of dictionaries with, "geo_accession", no_samples", "species","title", "study_summary"
```