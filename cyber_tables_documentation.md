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

### Initialising a table
Creating a new table object:
```Python
cyber_table = CyberTable()
```
