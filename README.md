# Open Reaction Database: Schema (ord-schema)

This repository contains the schema and data exploration scripts for [Open Reaction Database](https://docs.open-reaction-database.org).

## Installation

You **MUST** first install git-lfs and clone the [ord-data](https://github.com/open-reaction-database/ord-data) repo **BEFORE** installing the `ord-schema` package in this repo.

1. install and initialize git-lfs
2. clone ord-data and ord-schema
3. create ord-schema/.venv/
4. install ord-schema in venv
5. run expore_schema.py

```bash
$ curl -s https://packagecloud.io/install/repositories/github/git-lfs/script.python.sh | bash
$ git lfs install
$ git clone https://github.com/open-reaction-database/ord-data
$ git clone https://github.com/hobson/ord-schema
$ cd ord-schema
$ uv venv -p 3.12
$ source .venv/bin/activate
$ uv pip install -e .[docs,tests,examples]
$ python explore_data.py
```

If you make changes to the protocol buffer definitions, [install](https://grpc.io/docs/protoc-installation/) `protoc`
and run `./compile_proto_wrappers.sh` to rebuild the wrappers.

## Examples

The `examples/` directory contains examples of dataset creation and use. To run locally, install with:

```bash
cd examples
jupyter-lab
```

Click here to run the examples with Binder:
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/open-reaction-database/ord-schema/HEAD?labpath=examples)

## Conventions

### 1. convention: compound stoichiometry

##### Created: 2023.07.04

##### Last updated: 2023.07.04

##### Description: 
1. The preferred field for compound stoichiometry is the map `Compound.features` or `ProductCompound.features`.
2. The key should be "stoichiometric_coefficient" or "stoichiometric_ratio".
3. The value should be a `Data` message with its `float_value` representing the compound's stoichiometric 
coefficient or ratio.

##### Related links: 
[#683](https://github.com/open-reaction-database/ord-schema/issues/683) 
[#684](https://github.com/open-reaction-database/ord-schema/pull/684)
