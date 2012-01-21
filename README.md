FIXME add descriptions of error codes

# Hierophant
Hierophant (Pyrophant?) is a Python client library for the Veritable API. 

## Creating an instance of the API
The `veritable_connect` method returns an instance of the Veritable API. The API key should be kept in an environment variable.

    VERITABLE_API_KEY = os.getenv("VERITABLE_API_KEY")
    API = hierophant.veritable_connect(VERITABLE_API_KEY)

## Listing the tables available to the user
The `tables` method of the API object returns a list of Table objects corresponding to the tables available to the user.

    tables = API.tables()
    
## Creating a new table
To create a new empty table with an automatically assigned id and a blank description, just call the `create_table` method of the API with no arguments.

    client_data = API.create_table()

Users can also specify the id and/or a long-form description of the table to make future access and reference easier.

    client_data = API.create_table(table_id = "client_data")
    client_data = API.create_table(table_id = "client_data", description = """Customer
                    interaction data from Client, Inc. for 3/2011 - 12/2011, 978k rows,
                    75 columns""")

FIXME what happens if we create a table that already exists

## Deleting a table
To delete an existing table, call its `delete` method.

    client_data.delete()

## Getting a table
To get the data associated with a table (for instance, to refresh its `last_updated` property), call its `get` method.

    client_data.get()

You can also retrieve a table by its user-specified id.

    client_data = API.get_table_by_id("client_data")
    
## Adding rows to a table
Users can specify the `_id` field of a row, or row names can be automatically assigned. Row names must be unique within each table.

    animals.add_row({'fuzzy': 'true', 'legs': '4', 'weight_kg': 3, 'tail': true, 'paws': true})
    client_data.add_row({'_id': ,})

    client_data.addrows(data)

## Retrieving and deleting rows from a table
    client_data.get_row("id")

Alternatively, you can get all of the rows belonging to a table at once using the batch `get_rows` method.

    client_data.get_rows()

To delete a row, just call the `delete_row` method with its id.

    client_data.delete_row("id")
    
FIXME what happens if we add a row that already exists.

## Specifying analyses
    analyses = client_data.get_analyses()
    
    analysis = client_data.create_analysis(schema, description, analysis_id, type)
    
    
    analysis.get()
    analysis.learn()
    analysis.delete()
    analysis.get_schema()

## Making predictions

    analysis.predict(request)
        