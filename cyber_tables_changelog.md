# Cyber Tables
## Changelog

### 205-06-29
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
- Add in functions to return values for use in matplotlib and seaborn
- Add in dict support for aggregation functions
- Add calculation column option -> days between dates / datetimes
- Replace repeated code with functions

## WIP
_internal_return_rows_by_value_recursive(self, input_rows:dict, column_indexes:list, values:list):
values being a list of lists allows for structured use of logic in the select function: < <= > >= = !=.    
Updated select() to check data types for non != and = comparrisons and raise value error if not numerical.    
To test.   
calculation column added: days_between. Untested.
