FIXME what happens if we create a table that already exists
FIXME add descriptions of error codes
FIXME what happens if we add a row that already exists.

# Hierophant
Hierophant (Pyrophant?) is a Python client library for the Veritable API. 

## Handles and vanilla representations
Most of the methods in this library return lists of the form `[HandleObject, dict_representation]`. The handle objects provide methods for manipulating API objects, while the dicts contain full representations of the objects. Handles are stateless, and dict representations are snapshots.

## Creating an instance of the API
The `veritable_connect` method returns an instance of the Veritable API. This is the entry point for everything that follows. In general, the API key should be kept in an environment variable.

    VERITABLE_API_KEY = os.getenv("VERITABLE_API_KEY")
    API = hierophant.veritable_connect(VERITABLE_API_KEY)

## Listing the tables available to the user
The `get_tables` method of the API object returns a list corresponding to the tables available to the user.

    tables = API.get_tables()
    table_handles = [t[0] for t in tables]
    table_dicts = [t[1] for t in tables]

## Creating a new table
To create a new empty table with an automatically assigned id and a blank description, just call the `create_table` method of the API with no arguments.

    client_data = API.create_table()
    client_data_handle = client_data[0]
    client_data_dict = client_data[1]

Users can also specify the id and/or a long-form description of the table to make future access and reference easier.

    client_data = API.create_table(table_id = "client_data")
    client_data = API.create_table(table_id = "client_data", description = """Customer data from Client, Inc. for 3/2011 - 12/2011, 978k rows, 21 columns""")

## Getting an existing table
To get a snapshot of an existing table (for instance, to refresh its `last_updated` property), you can call the `get` method of its handle.
    
    client_data_dict = client_data_handle.get()

You can also retrieve a table by its user-specified id, or by its URL, using the `get_table_by_id` and `get_table_by_url` methods of the API object.

    client_data_dict = API.get_table_by_id("client_data")
    client_data_dict = API.get_table_by_url("https://api.priorknowledge.com/tables/client_data")

## Deleting a table
To delete an existing table, call the `delete` method of its handle

    client_data_handle.delete()

Alternatively, you can delete a table by its id or its URL, using the `delete_table_by_id` and `delete_table_by_url` methods of the API object.

    API.delete_table_by_id("client_data")
    API.delete_table_by_url("https://api.priorknowledge.com/tables/client_data")
    
## Adding rows to a table
Rows of a table are represented by dicts whose keys are the names of the columns of the table. Each row must have an `_id` field, which may be manually specified by the user or automatically assigned. These row ids must be unique within each table.

Rows can be added to a table by calling the `add_row` method of the handle object.

    animals_handle.add_row({'fuzzy': 'true', 'legs': '4', 'weight_kg': 3, 'tail': true, 'paws': true})
    animals_handle.add_row({'_id': 'cat', 'fuzzy': 'true', 'legs': '4', 'weight_kg': 3, 'tail': true, 'paws': true})

To add rows in bulk, use the `add_rows` method of the table handle. This method expects a list of dicts, each of which represents a single row of the table.
 
    client_data_handle.add_rows(client_data_rows)

## Retrieving and deleting rows from a table
To retrieve a row from a table by its id, use the `get_row_by_id` method of the table handle.

    animals_handle.get_row_by_id("cat")

You can also retrieve rows by their urls, using the `get_row_by_url` method.

    animals_handle.get_row_by_url("https://api.priorknowledge.com/tables/animals/rows/cat")

Alternatively, you can get all of the rows belonging to a table at once using the batch `get_rows` method.

    client_data_handle.get_rows()

To delete a row, just call the analogous `delete_row_by_id` or `delete_row_by_url` methods.

    animals_handle.delete_row_by_id("cat")
    animals_handle.delete_row_by_url("https://api.priorknowledge.com/tables/animals/rows/cat")

It's also possible to batch delete rows from the table, using the `delete_rows` method of the table handle. This is analogous to `add_rows` and again expects a list of dicts, each of which represents a single row of the table and must contain an `_id` field.

    client_data_handle.delete_rows(client_subset)

## Specifying new analyses of a table and performing inference
To set up a new analysis of a table, use the `create_analysis` method of the table handle. This method takes a `schema`, which must be a dict whose keys are column names (for some subset of the columns in the table---note that it is not necessary to analyze all of the columns in the table) and whose values are dicts:

   {'age': {'type': 'count'},
    'weight': {'type': 'real'},
    'diagnosis': {'type': 'categorical'},
    'medicaid': {'type': 'boolean'}}

To validate a schema, we provide the convenience function `validate_schema`. Note that this function does *not* presently check column names against the table.

The user can optionally pass a `description` and/or an `analysis_id` to the `create_analysis` method, or the id can be autogenerated. This method returns both a handle to the analysis and a dict description.

    my_analysis = client_data_handle.create_analysis(schema, description = "My analysis of the client data", analysis_id = "my_analysis", type = "veritable")
    my_analysis_handle = my_analysis[0]
    my_analysis_dict = my_analysis[1]

It is possible to create more than one analysis against a given table both because columns can be coded in multiple ways and because we may want to use Veritable to analyze different subsets of the columns of the table.

To start inference, just call the `learn` method of the analysis handle.

    my_analysis_handle.learn()

## Learning an analysis
## Retrieving and deleting analyses
To see all the analyses related to a given table, call the `get_analyses` method of the table handle. The return value is analogous to that of `get_tables`:

    analyses = client_data.get_analyses()
    analysis_handles = [a[0] for a in analyses]
    analysis_dicts = [a[1] for a in analyses]

You can also retrieve an analysis by its id or URL. Again, the return value is a list of a handle object and a dict description.
    
    my_analysis_handle = client_data.get_analysis_by_id("my_analysis")[0]
    my_analysis_handle = client_data.get_analysis_by_url("https://api.priorknowledge.com/tables/client_data/analyses/my_analysis")[0]

And it is possible to delete an analysis in the same way.
    
    client_data.delete_analysis_by_id("my_analysis")
    client_data.delete_analysis_by_url("https://api.priorknowledge.com/tables/client_data/analyses/my_analysis")

## Retrieving the description of an analysis or schema

Given an analysis handle, it is always possible to retrieve either its dict description or the corresponding schema, using the `get` and `get_schema` methods of the handle object.

    my_analysis_dict = my_analysis_handle.get()
    my_analysis_schema = my_analysis_handle.get_schema()


## Making predictions based on a completed analysis
A predictions request should be a dict with two entries. The `data` entry should contain a row specification, where the value of every conditioning column is specified and the value of every predicted column is `null`. The `count` entry should specify the number of predicted values requested.

    request = {'data': {'age': 30, 'weight': 'null'},
               'count': 2}
    results = my_analysis_handle.predict(request)

Predictions are synchronous!
