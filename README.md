FIXME add descriptions of error codes

# Veritable Python client
First things first.

For now, you can clone the git repo and then from within the repo directory run:

    pip install -r requirements.txt .

Then just:

    import veritable


## Resource access and resource state
This library distinguishes between _handles_ to API resources and the _state_ of those resources. Handles are objects that provide methods to access and manipulate resources. Resource state is represented by Python dict that map more closely onto the API's JSON response format. Handles are stateless, and dict representations are snapshots. A common pattern is therefore to get the handle of an object, and then get its state.


## Creating an instance of the API
The `veritable_connect` method returns an instance of the Veritable API. This is the entry point for everything that follows. In general, the API key should be kept in an environment variable.

    VERITABLE_API_KEY = os.getenv("VERITABLE_API_KEY")
    API = veritable.veritable_connect(VERITABLE_API_KEY)

If your instance of the API doesn't live at the default URL, `https://api.priorknowledge.com/`, then you can pass the entry point of the API to `veritable_connect` as its second argument.

    VERITABLE_API_BASE_URL = os.getenv("VERITABLE_API_BASE_URL")
    API = veritable.veritable_connect(VERITABLE_API_KEY, VERITABLE_API_BASE_URL)

Attempting to instantiate the API without passing an API key, or with no base URL, will cause an `APIKeyException` or `APIBaseURLException`, respectively, to be raised.

## Listing the tables available to the user
The `get_tables` method of the API object returns a list of table handles corresponding to the table resources currently available to the user.

    table_handles = API.get_tables()


## Creating a new table
To create a new empty table with an automatically assigned id and a blank description, just call the `create_table` method of the API with no arguments. This will return a handle to the new table.

    client_data_handle = API.create_table()

Users can also specify the id and/or a long-form description of the table to make future access and reference easier.

    client_data_handle = API.create_table(table_id = "client_data")
    client_data_handle = API.create_table(table_id = "client_data", description = """Customer data from Client, Inc. for 3/2011 - 12/2011, 978k rows, 21 columns""")

Attempting to create a table which shares its id with an existing table will cause a `DuplicateTableException` to be raised unless `force=True` is passed to the `create_table` method. If table creation is forced, the existing table with the same id will be overwritten.


## Getting the state of an existing table
To get a snapshot of an existing table (for instance, to refresh its `last_updated` property), you can call the `get_state` method of its handle.
    
    client_data_state = client_data_handle.get_state()


## Retrieving a previously created table
You can retrieve a previously created table by its user-specified id, or by its URL, using the `get_table_by_id` and `get_table_by_url` methods of the API object.

    client_data_handle = API.get_table_by_id("client_data")
    client_data_handle = API.get_table_by_url("https://api.priorknowledge.com/tables/client_data")


## Deleting a table
To delete an existing table, call the `delete` method of its handle

    client_data_handle.delete()

Alternatively, you can delete a table by its id or its URL, using the `delete_table_by_id` and `delete_table_by_url` methods of the API object.

    API.delete_table_by_id("client_data")
    API.delete_table_by_url("https://api.priorknowledge.com/tables/client_data")

Once a table has been deleted using one of these methods, attempting to perform further operations on it will cause a `DeletedTableException` to be raised.

## Adding rows to a table
Rows of a table are represented by dicts whose keys are the names of the columns of the table. Each row must have an `_id` field, which may be manually specified by the user or automatically assigned. These row ids must be unique within each table.

Rows can be added to a table by calling the `add_row` method of the handle object.

    animals_handle.add_row({'fuzzy': 'true', 'legs': '4', 'weight_kg': 3, 'tail': true, 'paws': true})
    animals_handle.add_row({'_id': 'cat', 'fuzzy': 'true', 'legs': '4', 'weight_kg': 3, 'tail': true, 'paws': true})

To add rows in bulk, use the `add_rows` method of the table handle. This method expects a list of dicts, each of which represents a single row of the table.
 
    client_data_handle.add_rows(client_data_rows)

Attempting to add rows which share their ids with existing rows will cause a `DuplicateRowException` to be raised unless `force=True` is passed to the `add_row` or `add_rows` method. If row creation is forced, existing rows with the same ids will be overwritten.


## Retrieving and deleting rows from a table
To retrieve a row from a table by its id, use the `get_row_by_id` method of the table handle.

    animals_handle.get_row_by_id("cat")

You can also retrieve rows by their urls, using the `get_row_by_url` method.

    animals_handle.get_row_by_url("https://api.priorknowledge.com/tables/animals/rows/cat")

Alternatively, you can get all of the rows belonging to a table at once using the batch `get_rows` method. This method returns a dict of the form `{'rows': [{'_id': 'row_1_id', ...}, ...]}`.

    client_data_handle.get_rows()

To delete a row, just call the analogous `delete_row_by_id` or `delete_row_by_url` methods.

    animals_handle.delete_row_by_id("cat")
    animals_handle.delete_row_by_url("https://api.priorknowledge.com/tables/animals/rows/cat")

It's also possible to batch delete rows from the table, using the `delete_rows` method of the table handle. This is analogous to `add_rows` and again expects a list of dicts, each of which represents a single row of the table and must contain an `_id` field. If this field is missing, a `MissingRowIDException` will be raised.

    client_data_handle.delete_rows(client_subset)


## Specifying new analyses of a table and performing inference
To set up a new analysis of a table, use the `create_analysis` method of the table handle. This method takes a `schema`, which must be a dict whose keys are column names (for some subset of the columns in the table--note that it is not necessary to analyze all of the columns in the table) and whose values are dicts:

    {'age': {'type': 'count'},
     'weight': {'type': 'real'},
     'diagnosis': {'type': 'categorical'},
     'medicaid': {'type': 'boolean'}}

To validate a schema, we provide the convenience function `validate_schema`, which raises a `InvalidSchemaException` if it does not succeed. Note that this function does *not* presently check column names against the table, and is *not* automatically run when an analysis is created.

The user can optionally pass a `description` and/or an `analysis_id` to the `create_analysis` method, or the id can be autogenerated. This method returns both a handle to the analysis and a dict description.

    my_analysis_handle = client_data_handle.create_analysis(schema, description = "My analysis of the client data", analysis_id = "my_analysis", type = "veritable")

It is possible to create more than one analysis against a given table both because columns can be coded in multiple ways and because we may want to use Veritable to analyze different subsets of the columns of the table.

Attempting to create analyses which share their ids with existing analyses will cause a `DuplicateAnalysisException` to be raised unless `force=True` is passed to the `create_analysis` method. If analysis creation is forced,  the existing analysis with the same id will be overwritten.

Do not change the `type` argument to `create_analysis` from its default value, `veritable`. Passing any other value will cause an `InvalidAnalysisTypeException` to be raised.


## Running an analysis

To start inference, just call the `run` method of the analysis handle.

    my_analysis_handle.run()


## Retrieving and deleting analyses
To see all the analyses related to a given table, call the `get_analyses` method of the table handle. The return value is analogous to that of `get_tables`:

    analysis_handles = client_data.get_analyses()

You can also retrieve an analysis by its id or URL. Again, the return value is a list of a handle object and a dict description.
    
    my_analysis_handle = client_data.get_analysis_by_id("my_analysis")
    my_analysis_handle = client_data.get_analysis_by_url("https://api.priorknowledge.com/tables/client_data/analyses/my_analysis")

And it is possible to delete an analysis in the same way.
    
    client_data.delete_analysis_by_id("my_analysis")
    client_data.delete_analysis_by_url("https://api.priorknowledge.com/tables/client_data/analyses/my_analysis")


## Retrieving the schema of an analysis
Given an analysis handle, it is always possible to retrieve the corresponding schema.

    my_analysis_schema = my_analysis_handle.get_schema()


## Checking the status of an analysis
Each analysis handle also allows you to check on the current state of the analysis.

    my_analysis_state = my_analysis_handle.get_state()

The state of an analysis is a Python dict with at least three entries, `type`, `state`, and `links`, and possibly the entries `last_learned` or `error`. The `type` will always be `veritable`. The `status` will always be one of the following values:

*  `new`: the analysis has never run
*  `pending`: the analysis is running and not yet finished
*  `finished`: the analysis is finished running, and ready for predictions
*  `failed`: the analysis has failed

The `links` entry will contain links to the related resources if available. The `last_learned` entry will be present after the analysis has been run at least once. You can compare this timestamp to the `last_updated` timestamp in the representation of the table state in order to make sure that the analysis reflects the latest data. (If not, just call the analysis's `run` method again.) 

If the analysis status is `failed`, then an `error` entry will also be present, and will describe the reason for failure.

For convenience, if you just want to check the status of an analysis, use its `status` method.

    my_analysis.status()


## Making predictions based on a completed analysis
A predictions request should be a dict with two entries. The `data` entry should contain a row specification, where the value of every conditioning column is specified and the value of every predicted column is `None`. The `count` entry should specify the number of predicted values requested.

    request = {'data': {'age': 30, 'weight': None},
               'count': 2}
    results = my_analysis_handle.predict(request)

Predictions are synchronous!
