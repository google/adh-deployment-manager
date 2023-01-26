 # ADH Deployment Manager

ADH Deployment Manager is a Python library which simplifies interfacing with ADH REST API by providing a convenient set of wrappers and abstractions.
The library provides such capabilities as:
* deploying ADH queries from local text files to multiple projects based on a single configuration file
* sync queries between ADH and local storage
* local runner for testing and prototyping
* batch query executor
* branching mechanism
* job monitoring and other features

ADH Deployment Manager provides both high level interface for interacting with ADH via Deployment object and low-level by providing access to such elements as *Analysys Query* and *Job* to issue ad-hoc operations (like rerun of a particular query).

**Minimal working example**:

```
# load necessary modules
from adh_deployment_manager.authenticator import AdhAutheticator
from adh_deployment_manager.deployment import Deployment
import adh_deployment_manager.commands as commands

# provide authentication mechanism
credentials = AdhAutheticator().get_credentials("/path/to/credentials.json")
developer_key =  "INSERT_YOUR_DEVELOPER_KEY"

# instantiate deployment with config and credentials
# (and optionally path to folder where source queries are located)
deployment = Deployment(
    config = "/path/to/config.yml",
    credentials = credentials,
    developer_key = developer_key,
    queries_folder="/path/to/adh-queries/",
    query_file_extention=".sql")

# deploy queries to ADH project(s)
deployer = commands.Deployer(deployment)
deployer.execute()

# run queries in ADH projects(s)
runner = commands.Runner(deployment)
runner.execute()
```

# Table of contents<a name="table-of-contents"></a>
1. [Project overview](#project-overview)
2. [Requirements](#requirements)
2. [Installation](#installation)
3. [Getting started](#getting-started)
    1. [Access setup](#access-setup)
	    1. [*(Recommended)* - Authenticating as a service account](#recommended-authenticating-as-a-service-account)
	    2. [OAuth 2.0 setup](#oauth-20-setup)
    2. [Create config](#create-config)
    3. [Specify queries](#specify-queries)
	    1. [Add new ADH queries](#add-adh-queries)
	    2. [Use existing ADH queries](#use-adh-queries)
    4. [Deploying and running queries](#running-queries)

## Project overview<a name="project-overview"></a>
*Back to [table of contents](#table-of-contents)*

ADH Deployment Manager deployment consists of two elements:

* `sql` folder - contains ADH queries in `.sql` format
* `config.yml` file - specifies which queries from `sql` folder should be deployed alongside parameters and filtered row summary. More about config at [Create Config](#create-config).

Check possible structure for `my_adh_project` deployment below:

```
my_adh_project
|__config.yml
|__sql
   |__query_name1.sql
   |__query_name2.sql
```

## Requirements<a name="requirements"></a>
*Back to [table of contents](#table-of-contents)*

* [Python3](https://www.python.org/downloads/)
* [Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)

## Installation<a name="installation"></a>
*Back to [table of contents](#table-of-contents)*

The CLI tool called `adm` can be installed from pip:

```
pip install adh-deployment-manager
```

## Getting started<a name="getting-started"></a>
*Back to [table of contents](#table-of-contents)*

### Access setup<a name="access-setup"></a>
Please follow [Get started with the Ads Data Hub API](https://developers.google.com/ads-data-hub/guides/quickstart-api) to correctly setup
API access to Ads Data Hub.

After you setup API access there are two options to authenticate - via service account or OAuth 2.0.

#### *(Recommended)* Authenticating as a service account<a name="recommended-authenticating-as-a-service-account"></a>

Authenticating as a service account is the recommended way of authentication for `adh-deployment-manager`.
Please follow the steps outlined below:

1. Log in into Google Cloud project that connected to your ADH account
2. Create service account in your output GCP
    * reference link: https://cloud.google.com/iam/docs/creating-managing-service-accounts
3. Download service account's credential (JSON format) in your local environment
    * reference link: https://cloud.google.com/iam/docs/creating-managing-service-account-keys
4. Assign BigQuery Admin role to the service account
    * reference link: https://cloud.google.com/iam/docs/granting-roles-to-service-accounts
5. Generate API Key (Developer Key for ADH)
    * reference link: https://cloud.google.com/docs/authentication/api-keys
6. Assign Analyst access role to the service account
    * reference link: https://developers.google.com/ads-data-hub/guides/assign-access-by-role

#### OAuth 2.0 setup<a name="oauth-20-setup"></a>

If authenticating via service account is not possible please follow the steps outlined below:

1. Log in into Google Cloud project that connected to your ADH account
2. Generate OAuth 2.0 Client ID and download credentials:
    * go to *API & Services - Credentials*, click *+ CREATE CREDENTIALS*
    and select *OAuth client ID*
    * Select *Desktop App* as application type, specify any application name
    and click *CREATE* button.
    * Click the download icon next to the credentials that you just created.
3. Generate API Key (Developer Key for ADH)
    * reference link: https://cloud.google.com/docs/authentication/api-keys

Once `adh-deployment-manager` is running you will be prompted to log in into your Google account
so the program can authenticate.

### Create config<a name="create-config"></a>
*Back to [table of contents](#table-of-contents)*

`config.yml` is the core element of deployment.
It must contain two mandatory elements:
* `customer_ids` - customer_ids (either one of an array) for which queries should be deployed and/or run.
* `queries_setup` - compound element which consists of query titles, parameters, filtered row summary, etc.

The minimal working example of the config with two requirement elements (`customer_ids` and `queries_setup`):
```
customer_ids:
  - 123456789
queries_setup:
  - queries:
    - query_title
```
#### Optional Elements

`config.yml` may contain optional elements that can be associated with all queries in  `queries_setup` block:

* `ads_data_from` - list of customer_ids to get ads data from. If the field is not included in config it will be automatically converted to a list of regular customer_ids.
* `bq_project` & `bq_dataset` - BQ project and dataset used storing output data (specified during ADH setup)
* `date_range_setup` - date range for running queries in ADH which consists of two elements: `start_date` and `end_date` in YYYY-MM-DD format (i.e., 1970-01-01). Supports template values, i.e. YYYYMMDD-10 transforms into *10 days ago from execution day*.

#### Specifying queries and their parameters


`queries_setup` may contains the following elements:

* `queries` - list of query titles that need to be deployed/launched in ADH. If query with specified title cannot be found in ADH, `adh-deployment-manager` will try to build query with such title based on provided filename in `sql` folder. Every `query_title` in `queries` will share the same `parameters` and `filtered_row_summary` provides for the block.
    * **`queries` should contain at least one query_title**.
    * `query_title` will be used to create table in BQ, so the output table will be `bq_project.bq_dataset.query_title`.
* (*optional*) `parameters` - block that contains one or more `parameter_name` with corresponding `type` and `values`.
	* `type` - type of the parameter (i.e. `INT64`, `STRING`, `DATE`, `TIMESTAMP`), required field.
	* `values` - values used when query is suppose to run. If you provide array structure here (separated by `-` at each line) `type` of parameter will be `ARRAY` of type `type`, optional field.

* (*optional*) `filtered_row_summary` - block that contains one or more filtered row summary column names with corresponding `type` and `value`.
    * `type` - type of filtered row summary (either `SUM` or `CONSTANT`)
    * `value` - specified only when `type` `CONTANT` is used, specifies how this metric or dimention will be named.

* (*optional*) `execution_mode` - option to split query execution and saving results by day. Can be either `normal` (query is run over the `start_date` - `end_date` date range) or `batch` (query execution can be splitted over each day within query `start_date` and `end_date`). `execution_mode` can be omitted, in that case the query will be executed in `normal` mode

* (*optional*) `wait` - specify whether the next query or query block should be launch only after successfull execution of the previous one. Can take two possible values: `each` (wait for each query in the block) or `block` (wait only for the last query in the block). if `wait` is omitted it means that query execution will be independent of the previous one.
* (*optional*) `replace` - if a query has any placeholders (specified in `{placeholder}` format) that `replace` block should contain *key: value* pairs which will replace placeholders in the query text with supplied values. This can be useful when specifing *bq_project* and *bq_dataset* names. `replace` can be omitted, in that case no replacements will be performed.
* (*optional*) `date_range_setup` - in case queries in a block should run over a different time period than specified in global `date_range_setup` you can specify these `start_date` and `end_date` here.


##### Example of queries_setup

The example structure of `queries_setup` looks like the following one:

```
queries_setup:
  - queries:
      - query_title1
      - query_title2
    parameters:
      parameter_name1:
        type: INT64
	values:
        - 1234
        - 1235
        - 1236
      parameter_name2:
        type: STRING
	values: my_value
      parameter_name3:
        type: DATE
    filtered_row_summary:
      metric_name:
        type: SUM
      dimension_name:
        type: CONSTANT
	value: my_value
    execution_mode: normal
    wait: each
    replace:
      placeholder1: value1
      placeholder2: value2
    date_range_setup:
      start_date: YYYYMMDD-10
      end_date: YYYYMMDD-1
  - queries:
    ....
```
In order to make the structure of config more clear, let's cover all elements in the example above.\
`queries_setup` contains a query block which contains two queries (`query_title1` and `query_title2`).

**Deploying**:\
For each of these queries:
* three parameters should be created:

    * `parameter_name1` of type `ARRAY` of `INT64` (three sample values are specified under `values` column; these values will be used during runtime)
    * `parameter_name2` of type `STRING` (with a single value `my_value` which will be used during runtime)
    * `parameter_name3` or type `DATE`. This parameters does not have value associated with it and should be specified during runtime (as keyword argument to a corresponding function call)
* Filtered Row Summary should be added:
    *  column `metric_name` will contain sum of all values filtered due to privacy checks
    *  column `dimension_name` will contain `my_value` for all users filtered due to privacy checks.

**Running**:
* Since `start_date: YYYYMMDD-10`, `end_date: YYYYMMDD-1` both queries should be executed over the last 10 days period (excluding today)
* Since `execution_mode: normal` when running these queries we run them from `start_date` to `end_date` period without splitting query execution by day.
* Since `wait: each` we launch `query_title2` only after `query_title1` execution is completed.
* Both queries contain two placeholders - `placeholder1` and `placeholder2`. When deploying them to ADH we will replace them with `value1` and `value2` respectively.


### Specify queries<a name="specify-queires"></a>
*Back to [table of contents](#table-of-contents)*

#### Add new ADH queries<a name="add-adh-queries"></a>

If the purpose of deployment is to create queries in ADH based on source code you need to create a dedicated folder to contain these queries.
By default `adh-deployment-manager` expectes `sql` folder with files containing files with `.sql` extension. Both `queries_folder` and `query_file_extention` could be specified when creating `Deployment` object.

```
from  adh_deployment_manager.deployment import Deployment

my_deployment = Deployment(
    config = "path/to/config.yml",
    credentials = my_credentials,
    queries_folder = "path/to/queries_folder",
    query_file_extention = ".sql"
    )
```

#### Use Existing Queries from ADH<a name="use-adh-queries"></a>

If the purpose of deployment is to run existing ADH queries you should omit `queries_folder` and `queries_file_extention` when creating `Deployment` object.
Query titles in `queries` block should be title of the queries found in ADH UI.
```
from  adh_deployment_manager.deployment import Deployment

my_deployment = Deployment(
    config = "path/to/config.yml",
    credentials = my_credentials
    )
```

### Deploying and running queires<a name="running-queries"></a>
*Back to [table of contents](#table-of-contents)*

ADH Deployment Manager installs `adm` CLI tool that allows you to simplify interaction with the library.
`adm` accept several arguments:

*  `command` - one of `run`, `deploy`, `update`, `fetch`
*  `subcommand` - one of `deploy` or `update`
*  `-c path/to/config.yml` - specifies where config is located
*  `-q path/to/queries_folder` - specifies where folder with queries is located
*   `-l path/to/output_folder` - specified where queries fetched from ADH should be stored

In order to run this commands you'll need to export developer_key as environmental variable:

```
export ADH_DEVELOPER_KEY=<developer_key>
```

#### Usage
```
adm [OPTIONS] command subcommand
    options:
    -c path/to/config.yml
    -q path/to/queries_folder
    -l path/to/output_folder
```

#### Examples

*Deploy queries based on config*

```
adm -c path/to/config.yml -q path/to/queries deploy
```

*Run queries without deployment*

```
adm -c path/to/config.yml run
```

*Run and update queries*

```
adm -c path/to/config.yml -q path/to/queries run update
```

*Fetch queries from config and store in specified location*

```
adm -c path/to/config.yml -l path/to/output_folder fetch
```
