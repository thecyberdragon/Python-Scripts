# Cyber Tables
## Documentation    
    
### Overview
Cyber tables is a python module that aims to make importing and working with table data nice and easy with plain English syntax that works on logic similar to SQL. 
The intention is to create something that can be an alternative to something like pandas and integrate some bespoke requirements at the same time.  
Cyber tables is also free of non-native Python dependancies, allowing you to use it 'out of the box'.    

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
