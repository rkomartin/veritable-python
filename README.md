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

## Deleting a table
To delete an existing table, call its `delete` method.

    client_data.delete()

## Getting a table

    client_data.get()
    
## Adding rows to a table
Users can specify the `_id` field of a row, or row names can be automatically assigned. Row names must be unique within each table.

    client_data.add_row({})
    client_data.add_row({'_id': ,})

    client_data.addrows(data)

    client_data.get_row("id")

    client_data.get_rows()
        
    client_data.delete_row("id")

    analyses = client_data.get_analyses()
    
    analysis = client_data.create_analysis(schema, description, analysis_id, type)
    
    
    analysis.get()
    analysis.learn()
    analysis.delete()
    analysis.get_schema()
    analysis.predict(request)
        