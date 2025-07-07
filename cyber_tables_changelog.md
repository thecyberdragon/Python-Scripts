# Cyber Tables
## Changelog

### 2025-07-07
Update: open_csv() Ignore errors on string encode if not UTF-8    
New: Added function generate_static_column_data() to add in a single value to all tables    
New: Added rename_column() function to rename a column name    
New: Added print_row_detailed() function to print a single row mirroring the print method of print_columns()    
Bug: return_sub_row_by_index() used wrong index in return_row_items_by_index() function. Fixed     
Update: Added null_excess_columns paramater into open_csv() to automatically insert "NULL" in columns with no data    
Added: replace_string_data_in_column() to replace values in a column if the column is a string    

### 2025-07-04
Organisation: Refactored all code
New: Added in dict support for aggregation functions in CyberTable and CyberTableGroup objects.    
New: Added in 'days_between' calculation column option to return the days between two dates / datetimes. Needs testing.    
New: Within function return_sub_table_by_row_filters(), if 'values' is a 2x item list of lists, "=", "!=", "<", "<=", ">", ">=" can be used to filter rows by the second list item instead of the default: ==. Example: column_indexes=[1,2] values=[["<", 50], ["!=", "True"]] <- filter where column index 1 is less than 50, and column index 2 is not True. non != and = logic can only be performed on numeric or datetime columns.        
Update: select() now has where_by_index and where_by_name dictionary commands that get passed to the above return_sub_table_by_row_filters() function.    
Update: print_data_overview() has an optional filter option which will only print column stats from within that filter. Options: ["numeric", "string", "bool", "date"].  
Update: data overview filters added to help() print.    
Added: Numerous _internal functions for the refactoring process. 
Added: Row object: get_items().    
Added: Column object: get_index(), get_data_type(), get_name(), get_analyse_property().    
Added: Column object: get_longest_value() and longest_value property for probably later implementation.    
Added: CyberTable object: return_column_count(), return_row_count(), return_last_row_index().    

### 2015-06-29
New: Added select() function to print select columns with filters to mimic SQL statements.    
New: Added help() to feed back options for calculation columns and aggegation.    
New: Added set_column_as_static_value() to add a single value to all rows in one column.    
Bug: Fixed issue returning mean from a string column.    
New: Added return_covariance() to calculate covariance between two columns.    
New: Added return_correlation_coefficient() to calculate the correlation coefficient of two columns.    
New: Added find_meantingful_correlations() to check all numerical columns for a meaningful coefficient.     

### 2025-06-26    
Bug: Added in code to read_csv to check for quotations to handle values containing the comma delimiter.    
Organisation: Reorganised all functions into categories grouping by internal and non internal functions.    
Organisation: Added in type annotation to functions that return values.     
New: Added internal function _internal_validate_return_column_indexes() to clean up some code later.    
New: Added in round_trip_csv() function to open a csv, clean it, convert any ISO 8601 to datetime and save as a _cleaned file.    

### 2025-06-25
Bug: ISO 8601 detection doesn't account for milliseconds - Fixed. Now checks the string length and returns a slice if length = 23.    
Bug: Returned sub tables didn't reset row or column indexes - Fixed. Reurning a sub table always resets all indexes.    

## Current plans
- Add functionality to track the longest item in a column to easily display columns in maximum width
- Add in dunder methods to make some features easier to use
- Do bug testing on a normal data analysis
- Add in a way to allow users to create custom calculation columns without using preset functions
- Add in functions to return values for use in matplotlib and seaborn

## WIP
 
