# Cyber Tables
## Changelog

### 205-06-29
New: Added new select() function to print select columns with filters to mimic SQL statements.    

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
- Calculate covariance
- Calculate correlation coefficient
- Add in dict support for aggregation functions
- Add calculation column option -> days between dates / datetimes
- Add in static column value inserting
- Add in general data overview function
- Replace repeated code with functions
