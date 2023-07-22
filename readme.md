Building a map reduce based sql engine

For load query, we make sure that the table and the DB are present, if not we create them. A new schema file is created in Hadoop file system as a index file, which contains schema of all tables in the DB


For delete query, we remove the DB / table file from the Hadoop file system

For select query, the schema is read from the index file (which contains schema of all tables in the DB). Then, the project columns are identified using the given query. 
There are 2 control flows for the select statement : 

With the where clause : The select column is identified from the query and the conditional operator (for filtering) is identified as well.

Without the where clause : No additional thing is required here.

For aggregate queries, the aggregate function is identified.

Finally, the select, project indices; the aggr function; the column names are passed to the mapper. 

The mapper iterates through all the rows and does a filter based on the values of the column specified in the where part. 

The reducer iterates through all the elements in it’s input (outputs of the map function), and aggregates them based on the aggr function.

The final result from the reducer is printed to the console.
