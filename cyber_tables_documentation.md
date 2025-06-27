# Cyber Tables
## Documentation    
    
### Overview
Cyber tables is a python module that aims to make importing and working with table data nice and easy with plain English syntax.    
The intention is to create something that can be an alternative to something like pandas and integrate some bespoke requirements at the same time.    
Cyber tables is also free of non-native Python dependancies, allowing you to use it 'out of the box'.    

### Structure
Cyber tables is made up of four classes / objects:
- Row: A row object contains a unique index for that row and the items in that row as a list
- Column: A column object contains a unique index for that column, the data type, name and other properties
- CyberTable: A table object made up of Column and Row objects
- CyberTableGroup: An object that serves to store multiple tables with identical column configurations for aggregation

### Data types
.
