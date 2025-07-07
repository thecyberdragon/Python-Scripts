# Cyber Tables
## Documentation    
    
### Overview
Cyber tables is a python module that aims to make importing and working with table data nice and easy with plain English syntax that works on logic similar to SQL. 
The intention is to create something that can be an alternative to something like pandas and integrate some bespoke requirements at the same time.  
Cyber tables is also free of non-native Python dependancies, allowing you to use it 'out of the box'.    
In the cyber_tables code are numerous functions beginning with _internal, they are only to be used if you know what you're doing. They are designed to only be used by normally-named functions.    

### Helper Code
When importing the module or using the script directly, you can print out the options for aggregation and calculation columns, and overview filters.
```Python
help()
```

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

### Initialising a table
Opening a CSV as a cyber_table
```Python
import cyber_tables

cyber_table = open_csv("file_path", delimiter = ",")
```

Return a copy of a table
```Python
new_table = cyber_table.return_copy()
```

### Saving a table as a CSV
```Python
# Save a cyber table as a CSV in a directory with a specific name (do not include .csv)
cyber_table.save_as_csv(directory, file_name, delimiter = ",")
```

### Automatic cleaning
Cyber tables provides a way to import a CSV, replace all empty values with NULL and optionally convert all ISO 8601 datetimes to a normal YYYY-MM-DD HH:MM:SS date time format. This function will output the CSV next to the original using the file name: filename_cleaned.csv.
```Python
round_trip_csv("file_path", delimiter = ",", convert_iso_8601 = True)
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

# Loop through all columns and perform overview calculations depending on the data type to help give you an overview of the data
# Options: ["all", "numeric", "string", "bool", "date"]
cyber_table.print_data_overview(filter='all')

# Return the number of columns
n = cyber_table.return_column_count() -> int

# Return the number of rows
n = cyber_table.return_row_count() -> int
```

### SQL-style select
This code is designed to mimic the SQL syntax un function form.    
- Column names / indexes -> The columns to show in the final print
- where_by_index / name -> dictionary to specify match conditions such as {"name":"dennis", "age":24}
- order_by -> column indexes in order of priority to order the table data
- order_mode -> asc for ascending, desc for descending
- limit -> limit the number of printed rows
- return_subtable -> returns a CyberTable object at the end
```Python
# Prints select columns and table rows where conditions are met with optional limit, and option to return a new table object
cyber_table.select(column_indexes = [], column_names = [], where_by_index = {}, where_by_name = {}, order_by = [], order_mode = "asc", limit = None, return_subtable = False)

# Example
cyber_table.select(column_indexes=[1,5,7,8], where_by_index = {2:"dennis", 5:True, 11:24}, order_by = [2, 3], order_mode = "desc", limit = 20)
# Print columns 1, 5, 7 and 8 where column 2 = "dennis", column 5 = True and column 11 = 24, order by column 2, then column 3 descending, and only print the top 20 items

# You can change the logical comparrison from == if instead of using "age":24, you make the comparrison value a 2 item list with item 1 being the prefered comparrison.    
# "age":24 -> age == 24, "age":["<",24] -> age < 24. By default, == is the comparrison used unless a valid 2-item list is used in place of a single value.

# Options
# "=", "!=", "<", "<=", ">", ">="
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

# Lock the data type of a column so that analyse_columns ignores it
cyber_table.lock_column_data_type(column_index = n, column_name = "name")

# Unlock the data type of a column so that analyse_columns doesn't ignore it
cyber_table.unlock_column_data_type(column_index = n, column_name = "name")

# Change the column data type (reference the options at the top of this page). Much be compatible with the conversion to said data type.
cyber_table.change_column_data_type("data_type", column_index = n, column_name = "name")

# Analyse columns and prescribe data types automatically where the column data types aren't locked
cyber_table.analyse_columns(column_index = n, column_name = "name")

# Update the data in a column using a list. The list must equal the number of rows in the table and be in update order
cyber_table.update_data_in_column(data:list, column_index = None, column_name = "name", auto_analyse = True/False)

# Adding a single static value to all rows under a column
cyber_table.set_column_as_static_value("static value", column_index = n, column_name = "name", auto_analyse = True)
```

Removing columns or values by column
```Python
# Remove a column
cyber_table.remove_column(index = n, name = "name") -> Column

# Remove data from all rows in a column's position, popping all items in all rows removing that column from the data.
cyber_table.remove_row_data_by_column_index(n)
```

Renaming a column
```Python
# Remove a column by the index or column name
cyber_table.rename_column(new_name, column_index = n, column_name = "name")
```

Returning column values
```Python
# Return the index of a column using the name. Returns as None if the name is not found.
cyber_table.return_column_index_by_name("name") -> int

# Returns all column data as a list
cyber_table.return_column_data(column_index = n, column_name = "name", include_nulls = True/False) -> list

# Returns the object of a column using the index
cyber_table.return_column_object_by_index(n) -> Column

# Returns a count of all True values in a bool column
cyber_table.return_true_count_from_column(column_index = n, column_name = "name") -> int

# Returns a count of all False values in a bool column
cyber_table.return_false_count_from_column(column_index = n, column_name = "name") -> int

# Return the column index based on the index or name if the column index/name exists
cyber_table.check_and_return_column_index(column_index = n, column_name = "name") -> int
```

Returning values based on a column
```Python
# Returns a new cyber_table where all rows in the column index = "NULL"
cyber_table.return_table_by_nulls_in_column(column_index = n, column_name = "name")
```

### Rows
Adding rows
```Python
# Add a new row
cyber_table.add_row(row:list)
```

Updating rows
```Python
# Update a row. The updated list must be the same length as the column count
cyber_table.update_row(row_index, updated_items)

# Add a static value to all rows in a specific column
cyber_table.generate_static_column_data(value, column_index = n, column_name = "name", auto_analyse = True)
```

Removing rows
```Python
# Remove rows where the rows contain the value in argument one is present in a specific column
cyber_table.remove_rows_by_column_value(value, column_index = n, column_name = "name")

# Remove a row by the row index
cyber_table.remove_row_by_index(self, index:int)
```

Returning row values
```Python
# Return the items of a row by the row index
cyber_table.return_row_items_by_index(index:int) -> list

# Return the rows in the table as a list of lists
cyber_table.return_rows_as_lists() -> list[list]

# Return a list of items in a row excluding columns not specified
cyber_table.return_sub_row_by_index(row_index:int, column_indexes = [], column_names = []) -> list

# Print a detailed breakdown of a row by the column name, index and data type
cyber_table.print_row_detailed(row_index = n)
```

Return values based on rows
```Python
# Return a list of all unique values in a column. "NULL" can be included or excluded, and you can optionally sort the list alphabetically
cyber_table.return_distinct_column_values(column_index = n, column_name = "name", include_nulls = False/True, sort = True/False) -> list
```

Ordering rows
```Python
# You can order the table alphanumerically based on the values in a specific column
cyber_table.order_rows_by_column(column_index = n, column_name = "name", mode = "asc"/"desc")
```

### Handling duplicates
Both dunctions return the number of indexes removed and a list of those indexes.
```Python
# Remove duplicate rows
cyber_table.remove_duplicate_rows() -> int, list[int]

# Remove duplicates within a selection of columns
cyber_table.remove_duplicate_rows_by_columns(column_indexes = [], column_names = []) -> int, list[int]
```

### Returning sub tables
Sub tables are just a term of another CyberTable object that has a reduced column or row count compared to the original.
```Python
# Return a sub table with select columns from the original table
new_table = cyber_table.return_sub_table_by_columns(column_indexes = [], column_names = []) -> CyberTable

# Return a cub table where items in the values list occur within select columns. All conditions must be true for a row to be returned.
# For example, values = [1, 2], column_indexes = [4, 7] would return a sub table for all rows where column index 4 = 1, and column index 7 = 2.
# Using the command dict will take priority over the lists that perform the same function. command_dict = {4:1, 7:2} achieves the same as the line above.   
new_table = cyber_table.return_sub_table_by_row_filters(self, values:list , command_dict = {}, column_indexes = [], column_names = []) -> CyberTable
```

### Returning calculations
Minimum, maximum and range. 
```Python
# Return the maximum value
# Accepted data types: string, int, decimal, bool, date, datetime
cyber_table.return_max_value(column_index = n, column_name = "name")

# Return the minimum value
# Accepted data types: string, int, decimal, bool, date, datetime
cyber_table.return_min_value(column_index = n, column_name = "name")

# Return the range between the minimum and maximum value
# Accepted data types: string, int, decimal, bool, date, datetime
cyber_table.return_range(column_index = n, column_name = "name")
```

Average calculations
```Python
# Return the mean
# Accepted data types: int, decimal, string
cyber_table.return_mean(column_index = n, column_name = "name")

# Return the median
# Accepted data types: int, decimal, string, date, datetime
cyber_table.return_median(column_index = n, column_name = "name")

# Return the mode
# Accepted data types: int, decimal, string, date, datetime, bool
cyber_table.return_mode(column_index = n, column_name = "name")
```

Sum calculations
```Python
# Return the sum
# Accepted data types: int, decimal
cyber_table.return_sum(column_index = n, column_name = "name")

# Return a count of all "NULL" values
# Accepted data types: int, decimal, string, date, datetime, bool, NULL
cyber_table.return_null_count(column_index = n, column_name = "name") -> int

# Return a counf of all non-"NULL" values
# Accepted data types: int, decimal, string, date, datetime, bool, NULL
cyber_table.return_non_null_count(column_index = n, column_name = "name") -> int
```

Statistics
```Python
# Return the variance
# Accepted data types: int, decimal
cyber_table.return_variance(column_index = n, column_name = "name")

# Returns the standard deviation
# Accepted data types: int, decimal
cyber_table.return_standard_deviation(column_index = n, column_name = "name")

# Returns the covariance between two numeircal columns
# Accepted data types: int, decimal
cyber_table.return_covariance(column_indexes = [], column_names = []) -> float

# Returns the correlation coefficient of two numerical columns
# Accepted data types: int, decimal
cyber_table.return_correlation_coefficient(column_indexes = [], column_names = [])

# Checks all numerical columns against all other numerical columns and returns pairings with <= -0.25 or >= 0.25 correlation coefficient
cyber_table.find_meantingful_correlations()

```

### Calculation columns
calculation_column_options = ["ntile", "rank", "individual_std", "individual_variance", "row_number", "+ days", "- days", "days_between", "above_threshold_percent", "below_threshold_percent"]

```Python
# Add a new column with a calculation on each row based on the reference column
cyber_table.add_calculation_column(reference_column_index = n, reference_column_name = "name", calculation = None, calculation_value = None)
```

- ntile: give rows an ntile bucket based on the number of buckets specified in the calculation_value argument
- rank: gives each row a rank based on the order minimum to maximum of the reference column
- individual_variance: gives each row a variance value based on the reference column
- individual_std: gives each row a standard deviation value based on the reference column
- row_number: gives each row a number top to bottom in default order
- \+ days: adds n days to a date or datetime based on the value given in the calculation_value argument
- \- days: subtracts n days from a date or datetime based on the value given in the calculation_value argument
- days_between: finds the number of days between two date or datetimes
- above_theshold_percent: gives each row a True or False value if the reference column falls in the top n percent given in the calculation_value argument
- below_threshold_percent: gives each row a True or False value if the reference column falls in the bottom n percent given in the calculation_value argument

### String functions

```Python
# Clean a string column by removing white space and optionally capitalising the first letter
cyber_table.clean_string_column(column_index = n, column_name = "name", capital_first_letter = True/False)

# Set the case of a string
# Options: lower, upper, title, lower_snake, upper_snake, title_snake
cyber_table.set_column_string_case(case, column_index = n, column_name = "name")

# Check and convert any ISO 8601 date time values to a normal datetime and change the data type
# Example os ISO 8601: 2025-06-27T12:01:05.142Z
cyber_table.convert_iso_8601_string_to_datetime(column_index = n, column_name = "name")
```

### Aggregating table data

Aggregation calculation options = ["sum", "mean", "mode", "median", "max", "min", "nulls", "non_nulls", "row_counts", "standard_deviation", "variance", "range", "true_percentage", "false_percentage"]      

The aggregate function will:
- Create a CyberTableGroup containing tables of all unique values from the combination of reference coluns
- Itterate through all columns in the calculation column list
- Perform a calculation for the corresponding calculation in the list of calculations
- Return a CyberTable consisting of all reference columns and the following calculations

The count of calculation columns must match the count of calculations. 

```Python
# Return a cyber table with aggregated data
aggregated_table = cyber_table.aggregate(command_dict = {}, reference_column_indexes = [], reference_column_names = [], calculation_column_indexes = [], calculation_column_names = [], calculations = []) -> CyberTable

# Example -> By all unique values in columns 0 and 1, calculate the mean of column 4 and the sum of column 7
aggregated_table = cyber_table.aggregate(reference_column_indexes = [0, 1],calculation_column_indexes = [4, 7], calculations = ["mean", "sum"])

# Example -> Performs the same as the example above but using the command dictionary.
aggregated_table = cyber_table.aggregate(reference_column_indexes = [0, 1], command_dict = {4:"mean", 7:"sum"})
```
Aggregation descriptions:
- sum: add up all values
- mean: calculates the mean 
- median: calculates the median 
- mode: calculates the mode
- max: calculates the maximum value
- min: calculates the minimum value
- range: calculates the difference between the maximum and minimum values
- nulls: calculates the number of "NULL" values
- non_nulls: calculates the number of non-"NULL" values
- row_counts: calculates the number of rows
- variance: calculates the variance
- standard_deviation: calculates standard deviation
- true_percentage: calculates the percentage of non-"NULL" values that are True
- false_percentage: calculates the percentage of non-"NULL" values that are False

### CyberTableGroup
You can crate a group object manually and then run functions on it for increased flexibility.

```Python
# Return a CyberTableGroup object containing tables for every unique combination of values in the list of columns specified.
# Example: column_indexes = [1, 2] will return an object containing a table for everyunique combination of values in column indexes 1 and 2.
cyber_group = cyber_table.return_groups(column_indexes = [], column_names = []) -> CyberTableGroup
```
Adding a table to the group
```Python
# Add a CyberTable object to the group, and specify which columns the table should be grouped by (must match the existing group)
cyber_group.add_table(table, group_indexes = [])
```

Returning tables from a group
```Python
# Return tables as a list of CyberTable objects
cyber_groups.return_tables()
```

Merge into CyberTable object
```Python
cyber_table = cyber_groups.merge_into_cyber_table()
```

View items in all tables
```Python
# Return the top n rows of all tables
cyber_groups.top(n)

# Return the bottom n rows of all tables
cyber_groups.bottom(n)

# Return a random selection of n rows from all tables
cyber_groups.random_selection(n)
```
Add a new calculation column to all tables    

Adding a new calculation column to all tables. Identical to the calculation column code for the CyberTable object.     
This simple calls that function on all tables in the group.    

- g_reference_column: the column to be used in the calculation
- g_calculation: the calculation to be done
- g_calculation_value: if the calculation needs additional input, use this argument ()
  
```Python
cyber_groups.add_batch_row_calculations(g_reference_column_index = n, g_reference_column_name = "name", g_calculation = None, g_calculation_value = None)
```

Aggregate a group and return a CyberTable
This is identical to the syntax and structure of the aggregate function in the CyberTable object. The CyberTable aggregate function simply calls this function.
```Python
cyber_table = cyber_groups.aggregate(reference_column_indexes = [], reference_column_names = [], calculation_column_indexes = [], calculation_column_names = [],  calculations = []) -> CyberTable
```
