# Cyber Tables
## Documentation    
    
### Overview
Cyber tables is a python module that aims to make importing and working with table data nice and easy with plain English syntax that works on logic similar to SQL. 
The intention is to create something that can be an alternative to something like pandas and integrate some bespoke requirements at the same time.  
Cyber tables is also free of non-native Python dependancies, allowing you to use it 'out of the box'.    
In the cyber_tables code are numerous functions beginning with _internal, they are only to be used if you know what you're doing. They are designed to only be used by normally-named functions.    

### Structure
Cyber tables is made up of four classes / objects:
- Row: A row object contains a unique index for that row and the items in that row as a list
- Column: A column object contains a unique index for that column, the data type, name and other properties
- CyberTable: A table object made up of Column and Row objects
- CyberTableGroup: An object that serves to store multiple tables with identical column configurations for aggregation

### Data types
- string -> Any non-NULL column that doesn't have another data type. Plain text
- NULL -> Applied to an entirely blank column
- bool -> True or False
- int -> Whole integers
- decimal -> Floating point numbers with a decimal point
- date -> Date values in the format YYYY-MM-DD
- datetime -> Date time values in the format YYYY-MM-DD HH:MM:SS

### Automatic cleaning
Cyber tables provides a way to import a CSV, replace all empty values with NULL and optionally convert all ISO 8601 datetimes to a normal YYYY-MM-DD HH:MM:SS date time format. This function will output the CSV next to the original using the file name: filename_cleaned.csv.
```Python
round_trip_csv("file_path", delimiter = ",", convert_iso_8601 = True)
```

### Initialising a table
Opening a CSV as a cyber_table
```Python
import cyber_tables

cyber_table = open_csv("file_path", delimiter = ",")
```

### Viewing table data
Cyber tables provides four ways to view the data in your table.
```Python
# View the top n rows in the table
cyber_table.top(n)

# View the bottom n rows in the table
cyber_tables.bottom(n)

# View a random n selection of rows in the table
cyber_tables.random_selection(n)

# View all rows in a table - Careful for large tables
cyber_tables.print()
```

### Viewing table properties
```Python
# Print all columns indexes, names, and data types
cyber_table.print_columns()

# Prints the above plus information plus the number of columns in the list, in the count property, how many rows there are and the row count property
cyber_table.print_structure()
```

### Indexes
In cyber_tables, indexes are the identifier in the dictionary within the table obejct to track the columns and rows. Indexes are unique, but may change if you morify the table by adding or removing columns or rows. Because of this, you should always check the indexes before using an index in a function call. When you create tables and sub-tables, the indexes get reset. 

```Python
# Reset column indexes manually
cyber_table.reset_column_indexes()

# Reset row indexes manually
cyber_table.reset_row_indexes()
```

### Function calls
As a general guide, when calling a function that needs a column or row reference, you will always have the option to use the column index(es) or the column name(s). In any situation whereby this choice is presented, if any indexes are given, those arguments will be prioritised and the names ignored. When referencing a column, either use the index(es) or the name(s), not a combination of both. 

### Columns
Adding columns
```Python
cyber_table.insert_column(self, name) -> int
cyber_table.insert_column_with_data(self, name:str, data:list, auto_analyse = True)
```

Updating columns
```Python
# Update column name
cyber_table.update_column_name(new_column_name, column_index = n, column_name = "name")
cyber_table.lock_column_data_type(column_index = n, column_name = "name")
cyber_table.unlock_column_data_type(column_index = n, column_name = "name")
cyber_table.change_column_data_type(new_data_type, column_index = n, column_name = "name")
cyber_table.analyse_columns(column_index = n, column_name = "name")
cyber_table.update_data_in_column(data:list, column_index = None, column_name = "name", auto_analyse = True/False)
```

Removing columns or values by column
```Python
cyber_table.remove_column(index = n, name = "name") -> Column
cyber_table.remove_row_data_by_column_index(n)
```

Returning column values
```Python
cyber_table.return_column_index_by_name("name") -> int
cyber_table.return_column_data(column_index = n, column_name = "name", include_nulls = True/False) -> list
cyber_table.return_column_object_by_index("name") -> Column
cyber_table.return_true_count_from_column(column_index = n, column_name = "name") -> int
cyber_table.return_false_count_from_column(column_index = n, column_name = "name") -> int
```

Returning values based on a column
```Python
cyber_table.return_table_by_nulls_in_column(column_index = n, column_name = "name")
```
