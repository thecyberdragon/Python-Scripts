# Cyber Tables
## Changelog

### 2025-07-13
New: Added a timecode data type for string starting in HH:MM:SS     
New: Added a TimeCode class    
New: normalist_timecode_column() converts timecode column data to TimeCode objects    
New: Added the timecode data type to: min, max, range, mean, median, mode, sum, null and non_null calculations    
New: Added timecode to the select() function    
New: Added new logical comparisons "like" and "not_like" to select()    
Fix: Fixed mistake with the threshold calculation column names    
New: open_avid_ale() function added to open avid bin export files    
New: round_trip_avid_ale_to_csv() function added to save ALE files as a cleaned CSV    
New: column now has a return_copy() function   
Bug: Fixed issue with the CyberTable.return_copy() function altering the input columns    

### 2025-07-11
Bug: Fixed issue with saving CSVs with commas in the cell contents     
Bug: Fixed issue with remove_row_by_index decrementing the column count rather than row count and reset_longest_value to update that    
New: Added functionality to compare number of rows between two CyberTable objects using < <= > and >=
New: Added functionality to compare number of rows and the row contents between two CyberTable objects with == and !=    
New: Added functionality to calculate the modulus of a CyberTable row count using %    
New: Added functionality to combine two CybeTable objects of matching columns together with +    
New: Added functionality to cubtract matching rows from a second CybeTable object using new_table = table_1 - table_2        
New: Added support for len(cyber_table) to return the row count    
Update: Added reset_indexes bool to remove_row_by_index to prevent that happening     
New: Added in internal function to update longest_value property of a CyberTable    
Update: Updated the print functions to left align strings, bool and nulls, right align numbers and center align dates     
Update: Column widths now equal in prints according to the longest value in that column with white space padding    
Added: return_two_columns_data() to return two column data rows to make making charts easier    


### 2025-07-07
Update: open_csv() Ignore errors on string encode if not UTF-8 and trim_excess_columns = True added to function to handle wonky columns with no data    
New: Added function generate_static_column_data() to add in a single value to all tables    
New: Added rename_column() function to rename a column name    
New: Added print_row_detailed() function to print a single row mirroring the print method of print_columns()    
Bug: return_sub_row_by_index() used wrong index in return_row_items_by_index() function. Fixed     
Update: Added null_excess_columns paramater into open_csv() to automatically insert "NULL" in columns with no data    
Added: replace_string_data_in_column() to replace values in a column if the column is a string    
Bug: Fixed an issue with row filter recursion function having an == in the wrong place    

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
- Do bug testing on a normal data analysis
 
