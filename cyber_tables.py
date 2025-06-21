import os
import random
import math
from datetime import datetime, timedelta

#----------------------------
## Reading & saving in data
# cyber_table = open_csv("E:\Websites\Cyber Dragon\project_area\cyber_table\personality_dataset.csv")
# cyber_table.save_as_csv(directory, filename_no_ext)

#----------------------------
## Date types
# string
# NULL
# bool
# int
# decimal
# date
# datetime

#----------------------------
## Overview and descriptrion
# cyber_table.print_structure()
# cyber_table.print_columns()
# cyber_table.print()
# top_five_rows = cyber_table.top(5)
# bottom_five_rows = cyber_table.bottom(5)
# random_five = cyber_table.random_selection(5)
# column_data = cyber_table.return_column_data(0)

#----------------------------
## Column code
# new_index = cyber_table.insert_column("New name")
# column_index = cyber_table.return_column_index_by_name("name")
# cyber_table.update_data_in_column(new_list, new_index)
# cyber_table.insert_column_with_data("Bool Data", bool_list)
# cyber_table.remove_column(name="Stage_fear")
# cyber_table.remove_column(index = 2)
# cyber_table.reset_column_indexes()
# cyber_table.update_column_name(new_value, index)
# cyber_table.change_column_data_type("int", column_index=0)
# cyber_table.set_column_string_case(column_index=7, case="lower")
# cyber_table.clean_string_column(7, capital_first_latter=True)
# cyber_table.convert_iso_8601_string_to_datetime(11)
# column = cyber_table.return_column_object_by_index(0)
# column.lock_column_data_type()
# column.unlock_column_data_type()
# cyber_table.analyse_columns()

#----------------------------
## Sub Tables
# new_table = cyber_table.return_sub_table_by_columns([0,2,4,7])
# new_table = cyber_table.return_sub_table_by_row_filters(["Extrovert", True, "NULL"], column_names=["Personality", "Bool Data", "Going_outside"])
# second_table = cyber_table.return_copy()

#----------------------------
## Row Code
# cyber_table.reset_row_indexes()
# cyber_table.remove_rows_by_column_value(column_index=0, value=4.0)
# cyber_table.remove_row_by_index(0)
# items = new_table.return_row_items_by_index(0)
# distincts = cyber_table.return_distinct_column_values(1)
# removals, index_list = cyber_table.remove_duplicate_rows()
# removals, index_list = cyber_table.remove_duplicate_rows_by_columns(column_indexes=[1,4])
# cyber_table.order_rows_by_column(0, mode="asc")
# cyber_table.order_rows_by_column(0, mode="desc")
# row_list = cyber_table.return_rows_as_lists()
# null_check = cyber_table.return_table_by_nulls_in_column(3)

#----------------------------
## Calculations
# sum = cyber_table.return_sum(num)
# col_min = cyber_table.return_min_value(num)
# col_max = cyber_table.return_max_value(num)
# mode, occurences = cyber_table.return_mode(num)
# mean = cyber_table.return_mean(0)
# median = cyber_table.return_median(0)
# nulls = cyber_table.return_null_count(3)
# non_nulls = cyber_table.return_non_null_count(3)
# variance = cyber_table.return_variance(column)
# std = cyber_table.return_standard_deviation(column)
# range = cyber_table.return-range(column_index)
# trues = cyber_table.return_true_count_from_column(8)
# falses = cyber_table.return_false_count_from_column(8)
# cyber_table.add_calculation_column(0, calculation="individual_variance")
# cyber_table.add_calculation_column(0, calculation="individual_std")
# cyber_table.add_calculation_column(0, calculation="row_number")
# cyber_table.add_calculation_column(9, calculation="+ days", calculation_value=7)
# cyber_table.add_calculation_column(9, calculation="- days", calculation_value=7)
# cyber_table.add_calculation_column(9, calculation="above_threshold_percent", calculation_value= 50)
# cyber_table.add_calculation_column(9, calculation="below_threshold_percent", calculation_value= 50)
# cyber_table.add_calculation_column(0, calculation="rank")
# cyber_table.add_calculation_column(0, calculation="ntile", calculation_value= 4)
# cyber_table.add_calculation_column(0, calculation="false_percentage")
# cyber_table.add_calculation_column(0, calculation="true_percentage")
# aggregated_table = cyber_table.aggregate(reference_column_indexes = [], calculation_column_indexes = [],  calculations = [])

## Grouping
# groups = cyber_table.return_groups(column_indexes = [0, 4])
# merged_table = groups.merge_into_cyber_table()
# groups.top(5)
# groups.bottom(5)
# groups.random_selection(5)
# groups.add_batch_row_calculations(0, calculation="rank")
# aggregated_table = groups.aggregate(reference_column_indexes = [], calculation_column_indexes = [],  calculations = [])



class Column():
    def __init__(self, name, index, data_type = None):
        self.name = name.replace("\n", "").strip()
        self.data_type = data_type
        self.index = index    
        self.allow_analyse = True
    def set_index(self, index):
        self.index = index
    def set_data_type(self, data_type):
        self.data_type = data_type
    def set_name(self, name):
        self.name = name
    def lock_data_type(self):
        self.allow_analyse = False
    def unlock_data_type(self):
        self.allow_analyse = True
    def print(self):
        print(f"Index: {self.index}, Name: {self.name}, Data Type: {self.data_type}, Unlocked: {self.allow_analyse}")
        
        
    # Accepted data types = int, decimal, bool, date, datetime, string, null
        
class Row():
    def __init__(self, index, items):
        self.index = index
        self.items = items     
    def set_index(self, index):
        self.index = index
    def set_items(self, items):
        self.items = items  
    def print(self):
        print(f"Index: {self.index}, Items: {self.items}")

class CyberTableGroup():
    def __init__(self):
        self.table_count = 0
        self.groups = []
        self.columns = []
        self.grouped_indexes = []
        
    def add_table(self, table, group_indexes = []):          
        if self.grouped_indexes == []:
            self.grouped_indexes = group_indexes
            
        if self.table_count == 0:
            for idx, column in table.columns.items():
                name = column.name
                data_type = column.data_type
                permissions = column.allow_analyse
                
                new_column = Column(name, idx, data_type)
                new_column.allow_analyse = permissions                
                self.columns.append(new_column)
                
            self.groups.append(table)
            self.table_count += 1  
        else:
            result = self._internal_check_incoming_table(table)
            if result == True:
                self.groups.append(table)
                self.table_count += 1  
            else:
                raise ValueError(f"Incoming table does not match the column schema of the group column list")                      
        
    def return_tables(self) -> list:
        return self.groups    
    
    def aggregate(self, reference_column_indexes = [], reference_column_names = [], calculation_column_indexes = [], calculation_column_names = [],  calculations = []):
        options = ["sum", "mean", "mode", "median", "max", "min", "nulls", "non_nulls", "row_counts", "standard_deviation", "variance", "range", "true_percentage", "false_percentage"]      
                
        for calculation in calculations:
            if calculation not in options:
                raise KeyError(f"Input option {calculation} not in list of approved options: {options}")
            
        column_index_list = []
        calculation_column_index_list = []
        
        column_indexes = [column_object.index for column_object in self.columns]
        
        if reference_column_indexes != []:
            for index in reference_column_indexes:
                
                if index not in column_indexes:
                    raise ValueError(f"Input index {index} not in list of known column indexes")
                else:
                    column_index_list.append(index)
                
        elif reference_column_names != []:
            for reference_name in reference_column_names:
                found = False
                for column in self.columns:
                    name = column.name            
                    if name == reference_name:
                        found = True
                        column_index_list.append(idx)
                        break
                if found == False:
                    raise KeyError(f"Column name {reference_name} not found in list of known column names")
                
        if calculation_column_indexes != []:
            for index in calculation_column_indexes:
                if index not in column_indexes:
                    raise ValueError(f"Input index {index} not in list of known column indexes")
                else:
                    calculation_column_index_list.append(index)
                
        elif calculation_column_names != []:
            for calculation_name in calculation_column_names:
                found = False
                for column in self.columns:
                    name = column.name
                    idx = column.index            
                    if name == calculation_name:
                        found = True
                        column_index_list.append(idx)
                        break
                if found == False:
                    raise KeyError(f"Column name {calculation_name} not found in list of known column names")               
        
        if len(calculation_column_index_list) != len(calculations):
            raise KeyError(f"Count of columns {len(calculation_column_index_list)} does not match the number of calculations {len(calculations)}")
        for index in column_index_list:
            if index in calculation_column_index_list:
                raise KeyError(f"Reference columns cannot also be calculation columns, they bust be distinctly separate")            
        
        aggregate_cybertable = CyberTable()
        
        for idx in column_index_list:
            name = self.columns[idx].name
            aggregate_cybertable._internal_add_column(name)
            
        for idx in range(len(calculation_column_index_list)):
            calculation_column_index = calculation_column_index_list[idx]
            calculation = calculations[idx]
            new_name = self.columns[calculation_column_index].name + "_" + calculation           
            aggregate_cybertable._internal_add_column(new_name)

        row_index = 0
        for table in self.groups:
            aggregate_row = []
            top_row = table.rows[0]
            items = top_row.items
            
            for reference in column_index_list:
                aggregate_row.append(items[reference])
                
            for iteration in range(len(calculation_column_index_list)):
                calculation_idx = calculation_column_index_list[iteration]
                calculation = calculations[iteration]
                
                if calculation == "min":
                    min = table.return_min_value(calculation_idx)
                    aggregate_row.append(min)                
                elif calculation == "max":
                    max = table.return_max_value(calculation_idx)
                    aggregate_row.append(max)
                elif calculation == "nulls":
                    nulls = table.return_null_count(calculation_idx)
                    aggregate_row.append(nulls)
                elif calculation == "non_nulls":
                    non_nulls = table.return_non_null_count(calculation_idx)
                    aggregate_row.append(non_nulls)
                elif calculation == "mean":
                    mean = table.return_mean(calculation_idx)
                    aggregate_row.append(mean)
                elif calculation == "median":
                    median = table.return_median(calculation_idx)
                    aggregate_row.append(median)
                elif calculation == "mode":
                    mode = table.return_mode(calculation_idx)
                    aggregate_row.append(mode[0])
                elif calculation == "sum":
                    sum = table.return_sum(calculation_idx)
                    aggregate_row.append(sum)
                elif calculation == "row_counts":
                    aggregate_row.append(table.row_count)
                elif calculation == "standard_deviation":
                    std = table.return_standard_deviation(calculation_idx)
                    aggregate_row.append(std)
                elif calculation == "variance":
                    var = table.return_variance(calculation_idx)
                    aggregate_row.append(var)
                elif calculation == "range":
                    value_range = table.return_range(calculation_idx)
                    aggregate_row.append(value_range)
                elif calculation == "true_percentage":
                    true_count = table.return_true_count_from_column(calculation_idx)
                    false_count = table.return_false_count_from_column(calculation_idx)
                    total_values = true_count + false_count
                    percent = (true_count / total_values) * 100
                    aggregate_row.append(percent)
                elif calculation == "false_percentage":
                    false_count = table.return_false_count_from_column(calculation_idx)
                    true_count = table.return_true_count_from_column(calculation_idx)
                    total_values = true_count + false_count
                    percent = (false_count / total_values) * 100
                    aggregate_row.append(percent)
                
            new_row = Row(row_index, aggregate_row)            
            aggregate_cybertable._internal_add_row(new_row, row_index)
            row_index += 1
            
        aggregate_cybertable.analyse_columns()
        return aggregate_cybertable
              
    def add_batch_row_calculations(self, g_reference_column_index = None, g_reference_column_name = None, g_calculation = None, g_calculation_value = None):
        if self.table_count == 0:
            raise IndexError(f"There are no tables in the group")        
        for table in self.groups:
            table.add_calculation_column(reference_column_index = g_reference_column_index, reference_column_name = g_reference_column_name, calculation = g_calculation, calculation_value = g_calculation_value)
    
    def _internal_check_incoming_table(self, table):
        for idx, column in table.columns.items():
            name = column.name
            data_type = column.data_type
            
            list_index = None
            for column in self.columns:
                column_index = column.index
                if column_index == idx:
                    list_index = column_index
                    break
            
            if list_index is None:
                raise KeyError(f"Incoming table column indexes do not match the first table in the group")
            
            existing_column = self.columns[idx]
            existing_name = existing_column.name
            existing_data_type = existing_column.data_type
            
            if existing_name != name:
                raise KeyError(f"Incoming table column name {name} at index {idx} from new table does not match the column name {existing_name} from the group columns")
            if existing_data_type != data_type:
                raise KeyError(f"Incoming table column {name} data type of {data_type} does not match the existing data type {existing_data_type} from the group column of the same name")
        return True
        
    def merge_into_cyber_table(self):
        new_table = CyberTable()     
        
        for column in self.columns:
            new_table._internal_add_column(column)
            
        for table in self.groups:
            for row in table.rows.values():
                new_table.add_row(row.items)
                
        return new_table
    
    def top(self, number):
        if self.table_count == 0:
            raise IndexError(f"There are no tables in the group")
        for table in self.groups:
            table.top(number)
            
    def bottom(self, number):
        if self.table_count == 0:
            raise IndexError(f"There are no tables in the group")
        for table in self.groups:
            table.bottom(number)
            
    def random_selection(self, number):
        if self.table_count == 0:
            raise IndexError(f"There are no tables in the group")
        for table in self.groups:
            table.random_selection(number)
            
class CyberTable():
    def __init__(self):
        self.columns = {}
        self.rows = {}
        self.column_count = 0
        self.row_count = 0
        self.last_row_index = None
        
    ### Information
    def print_structure(self):
        print("\nPrinting cyber table structure")
        print(f"\n==== Columns ====")
        print(f"Column count: {self.column_count}")
        print(f"Count of columns: {len(self.columns)}")
        for column in self.columns.values():
            print(f"Column index: {column.index}, Name: {column.name}, Data Type: {column.data_type}")
        print(f"\n==== Rows ====")
        print(f"Row count: {self.row_count}")
        print(f"Count of rows: {len(self.rows)}")
        print(f"Last row index: {self.last_row_index}")

    def print_columns(self):
        print("\nPrinting columns")
        for column in self.columns.values():
            print(f"Index: {column.index}, Name: {column.name}, Data Type: {column.data_type}")

    def column_names(self):
        column_names = []
        for column in self.columns.values():
            column_names.append(column.name)
        return " | ".join(column_names)
    
    ### Columns
    def _internal_add_column(self, column):
        if type(column) is Column:
            self.columns[column.index] = column        
            self.column_count += 1
        elif type(column) is str:
            new_column = Column(column, self.column_count)           
            self.columns[new_column.index] = new_column        
            self.column_count += 1
        
    def update_column_name(self, new_column_name, column_index = None, column_name = None):
        if column_index is not None:
            if column_index not in self.columns.keys():
                raise ValueError(f"Column index {column_index} is not in the list of column indexes")
            column_object = self.columns[column_index]
            column_object.set_name(new_column_name)
            self.columns[column_index] = column_object
        elif column_name is not None:
            column_names = [col.name for col in self.columns.values()]
            if column_name not in column_names:
                raise ValueError(f"Column: {column_name} not in list of known column names")
            for idx, column in self.columns.items():
                if column.name == column_name:
                    column_object = self.columns[idx]
                    column_object.set_name(new_column_name)
                    self.columns[column_index] = column_object
                    return
    
    def return_column_index_by_name(self, column_name):
        for index, column_object in self.columns.items():
            name = column_object.name
            if name == column_name:
                return index        

    def return_column_object_by_index(self, index) -> Column:
        return self.columns[index]   
    
    def lock_column_data_type(self, column_index = None, column_name = None):
        if column_index is not None:
            if column_index not in self.columns.keys():
                raise ValueError(f"Column index {column_index} not in the list of known indexes")
            self.columns[column_index].lock_datatype()                
        if column_name is not None:
            column_index = self.return_column_index_by_name(column_name)
            self.columns[column_index].lock_datatype()     
            
    def unlock_column_data_type(self, column_index = None, column_name = None):
        if column_index is not None:
            if column_index not in self.columns.keys():
                raise ValueError(f"Column index {column_index} not in the list of known indexes")
            self.columns[column_index].unlock_datatype()                
        if column_name is not None:
            column_index = self.return_column_index_by_name(column_name)
            self.columns[column_index].unlock_datatype()     
            
    def change_column_data_type(self, new_data_type, column_index = None, column_name = None):
        accepted_types = ["string", "int", "decimal", "bool", "date", "datetime", "NULL"]
        if new_data_type not in accepted_types:
            raise ValueError(f"Data type input {new_data_type} not in list of approved data types\nAccepted types: {accepted_types}")
        
        if column_index is not None:
            if column_index not in self.columns.keys():
                raise ValueError(f"Column index {column_index} not in list of known indexes")
            index = column_index
            
        elif column_name is not None:            
            index = self.return_column_index_by_name(column_name)
            if index is None:
                raise ValueError(f"Column name {column_name} not found int list of column names")            
        
        if self._internal_check_datatype_before_conversion(index, new_data_type) == True:
            self._internal_set_column_data_as_datatype(index, new_data_type)            
            self.columns[index].lock_data_type()   
            self.columns[index].set_data_type(new_data_type)
        else:
            raise ValueError(f"Column index {index} failed pre-conversion checks for data type {new_data_type}")
            
    def _internal_check_datatype_before_conversion(self, index, data_type) -> bool:
        try:            
            for idx, row in self.rows.items():
                items = row.items
                old_value = items[index]
                
                if old_value == "NULL":
                    break
                
                if data_type == "string": items[index] = str(old_value)
                elif data_type == "int": items[index] = int(old_value)
                elif data_type == "decimal": items[index] = float(old_value)
                elif data_type == "bool": items[index] = bool(old_value)
                elif data_type == "date": items[index] = datetime.strptime(old_value, "%Y-%m-%d")
                elif data_type == "datetime": items[index] = datetime.strptime(old_value, "%Y-%m-%d %H:%M:%S")
                elif data_type == "NULL": items[index] = "NULL"                      
            return True    
        except:
            return False 
    
    def _internal_set_column_data_as_datatype(self, index, data_type):
        try:            
            for idx, row in self.rows.items():
                items = row.items
                old_value = items[index]
                
                if old_value == "NULL":
                    continue
                
                if data_type == "string": items[index] = str(old_value)
                elif data_type == "int": items[index] = int(old_value)
                elif data_type == "decimal": items[index] = float(old_value)
                elif data_type == "bool": items[index] = string_to_bool(old_value)
                elif data_type == "date" and type(old_value) is datetime: items[index] = datetime.strftime(old_value, "%Y-%m-%d")
                elif data_type == "datetime" and type(old_value) is datetime: items[index] = datetime.strftime(old_value, "%Y-%m-%d %H:%M:%S")
                elif data_type == "date": items[index] = datetime.strptime(old_value, "%Y-%m-%d")
                elif data_type == "datetime": items[index] = datetime.strptime(old_value, "%Y-%m-%d %H:%M:%S")
                elif data_type == "NULL": items[index] = "NULL"
                
                self.rows[idx].set_items(items)                
            
        except ValueError as ve:
            raise ValueError(f"Value Error setting data type for index {index} to type {data_type}\n{ve}")
        except TypeError as te:
            raise TypeError(f"Value Error setting data type for index {index} to type {data_type}\n{te}")
    
    def return_column_data(self, column_index = None, column_name = None, include_nulls = True):
        output_data = []
        if column_index is not None:
            if column_index not in self.columns:
                raise ValueError(f"Index {column_index} is not in the list of known column indexes")
            for idx, row_object in self.rows.items():   
                value = row_object.items[column_index]
                if include_nulls == False and value != "NULL":          
                    output_data.append(row_object.items[column_index])
                elif include_nulls == True:
                    output_data.append(row_object.items[column_index])
                    
        elif column_name is not None:
            for idx, column in self.columns:
                name = column.name
                if name == column_name:
                    for row_idx, row in self.rows.items():
                        value = row.items[idx]
                        if include_nulls == False and value != "NULL":
                            output_data.append(row.items[idx])
                        elif include_nulls == True:
                            output_data.append(row.items[idx])
                    break
        else:
            return None
        return output_data
    
    def analyse_columns(self, column_index = None, column_name = None):
        for column in self.columns.values():
            name = column.name
            index = column.index
            data_type = column.data_type
            allow_analyse = column.allow_analyse
            
            if column_index is not None and index != column_index:
                continue
            
            if column_name is not None and name != column_name:
                continue
            
            if allow_analyse == False:
                continue
          
            column_data = (row.items[index] for row in self.rows.values())
            checks = 0
            is_int_count = 0
            is_decimal_count = 0      
            is_bool_count = 0          
            is_date_count = 0
            is_datetime_count = 0
            null_count = 0
            for data in column_data:
              
                if data != "NULL":                        
                    if is_int(data): is_int_count += 1
                    elif is_decimal(data): is_decimal_count += 1      
                    elif is_bool(data): is_bool_count += 1
                    elif is_date(data): is_date_count += 1
                    elif is_datetime(data): is_datetime_count += 1
                    checks += 1 
                else: 
                    null_count += 1
                    checks += 1
                                 
            if checks == null_count: data_type = "NULL"
            elif checks == is_int_count + null_count: data_type = "int"
            elif checks == is_decimal_count + null_count: data_type = "decimal"
            elif checks == is_bool_count + null_count: data_type = "bool"
            elif checks == is_date_count + null_count: data_type = "date"
            elif checks == is_datetime_count + null_count: data_type = "datetime"
            else: data_type = "string"
            
            new_column = Column(name, index, data_type)
            self.columns[index] = new_column   
            
            self._internal_set_column_data_as_datatype(index, data_type)  
                
    def remove_row_data_by_column_index(self, removal_index):
        if removal_index > (self.column_count - 1):
            return None
        for index, row in self.rows.items():
            row_items = row.items
            row_items.pop(removal_index)
            self.rows[index].items = row_items
        
    def remove_column(self, index = None, name = None):
        if index is not None:
            self.column_count -= 1
            self.remove_row_data_by_column_index(index)
            return self.columns.pop(index)
        if name is not None:
            for index, column in self.columns.items():
                if column.name == name:
                    self.column_count -= 1
                    self.remove_row_data_by_column_index(index)
                    return self.columns.pop(index)   
                 
    def insert_column(self, name):
        self.reset_column_indexes()
        new_index = self.column_count
        new_column = Column(name, new_index)
        self.columns[new_index] = new_column
        self.column_count += 1 
        entrees_needed = self.row_count        
        null_data = ["NULL" for iteration in range(entrees_needed)]
        self._internal_insert_data_into_column(null_data, column_index=new_index)   
        return new_index
    
    def insert_column_with_data(self, name:str, data:list, auto_analyse = True):
        new_index = self.insert_column(name)
        self.update_data_in_column(data, column_index=new_index, auto_analyse=auto_analyse)        
           
    def _internal_insert_data_into_column(self, data:list, column_index = None, column_name = None):
        input_row_length = len(data)
        row_count = self.row_count        
        length_check = input_row_length == row_count       
        
        if length_check == False:
            raise ValueError(f"Input data of count {input_row_length} does not match the row count of the table: {self.row_count}")
                
        if column_index is not None:
            if column_index not in self.columns.keys():
                raise ValueError(f"Input column index {column_index} not in list of known column indexes")                
            
            for row_index, row_object in self.rows.items():
                new_data = data[row_index]
                row_data:list = row_object.items
                row_data.insert(column_index, new_data)   
                self.update_row(row_index, row_data)  
                
        elif column_name is not None:
            for idx, column_object in self.columns.items():
                column_object_name = column_object.name
                if column_name == column_object_name:
                    if idx not in self.columns.keys():
                        raise ValueError(f"Input column name {column_name} not in list of known columns")
                    for row_index, row_object in self.rows.items():
                        new_data = data[row_index]
                        row_data:list = row_object.items
                        row_data.insert(column_index, new_data)
                        self.update_row(row_index, row_data)
                        return
                    
    def update_data_in_column(self, data:list, column_index = None, column_name = None, auto_analyse = True):
        input_row_length = len(data)
        row_count = self.row_count        
        length_check = input_row_length == row_count       
        
        if length_check == False:
            raise ValueError(f"Input data of count {input_row_length} does not match the row count of the table: {self.row_count}")
        
        found_index = None          
        if column_index is not None:
            if column_index not in self.columns.keys():
                raise ValueError(f"Input column index {column_index} not in list of known column indexes")                
            found_index = column_index
            for row_index, row_object in self.rows.items():
                new_data = data[row_index]
                row_data:list = row_object.items
                row_data[column_index] = new_data
                self.update_row(row_index, row_data)          
           
        elif column_name is not None:
            for idx, column_object in self.columns.items():                
                column_object_name = column_object.name
                if column_name == column_object_name:
                    if idx not in self.columns.keys():
                        raise ValueError(f"Input column name {column_name} not in list of known columns")
                    for row_index, row_object in self.rows.items():
                        new_data = data[row_index]
                        row_data:list = row_object.items
                        row_data[idx] = new_data
                        self.update_row(row_index, row_data)
                    found_index = idx
                    break

        if auto_analyse == True:
            self.analyse_columns(found_index)         
        
    def return_table_by_nulls_in_column(self, column_index = None, column_name = None):
        index = self.check_and_return_column_index(column_index = column_index, column_name = column_index)
        if index is not None:
            copy = self.return_copy()
            indexes_to_remove = []
            for idx, row in copy.rows.items():
                items = row.items
                value = items[index]
                if value != "NULL":
                    indexes_to_remove.append(idx)
                    
            for idx in indexes_to_remove:         
                copy.rows.pop(idx)
            copy.reset_row_indexes()
            return copy
      
    def return_true_count_from_column(self, column_index = None, column_name = None):
        index = self.check_and_return_column_index(column_index=column_index, column_name=column_name)  
        if index is not None:
            column = self.columns[index]
            data_type = column.data_type
            if data_type != "bool":
                raise TypeError(f"Cannot calculate true / false values from a non bool column")
            
            counter = 0
            for row in self.rows.values():
                value = row.items[index]
                if value == True:
                    counter += 1
            return counter                
            
    def return_false_count_from_column(self, column_index = None, column_name = None):
        index = self.check_and_return_column_index(column_index=column_index, column_name=column_name)  
        if index is not None:
            column = self.columns[index]
            data_type = column.data_type
            if data_type != "bool":
                raise TypeError(f"Cannot calculate true / false values from a non bool column")
            
            counter = 0
            for row in self.rows.values():
                value = row.items[index]
                if value == False:
                    counter += 1
            return counter       

    ### Rows    
    def add_row(self, row):
        if self.last_row_index is None:
            new_index = 0
            self.last_row_index = 0
        else:
            new_index = self.last_row_index + 1
            self.last_row_index += 1                
        cleaned_row = replace_missing_with_nulls(row)
        row_object = Row(new_index, cleaned_row)
        self.rows[new_index] = row_object
        self.row_count += 1
    
    def _internal_add_row(self, row:Row, index = None):       
        if index is not None:
            self.rows[index] = row
        else:
            self.reset_row_indexes()
            new_index = self.last_row_index + 1
            self.rows[new_index] = row      
    
    def update_row(self, row_index, updated_items):
        row_object = self.rows[row_index]
        row_object.items = updated_items
        self.rows[row_index] = row_object
    
    def _internal_return_rows_by_value_recursive(self, input_rows:dict, column_indexes:list, values:list):
        if column_indexes == []:
            return input_rows
        
        column_index = column_indexes[0]
        value_filter = values[0]
        
        filtered_rows = {}
        for idx, row in input_rows.items():
            items = row.items            
            if items[column_index] == value_filter:
                filtered_rows[idx] = row
        
        if len(column_indexes) == 1:
            return self._internal_return_rows_by_value_recursive(filtered_rows, [], [])
        else:
            return self._internal_return_rows_by_value_recursive(filtered_rows, column_indexes[1:], values[1:])

    def remove_rows_by_column_value(self, value, column_index = None, column_name = None):
        set_index = None
        
        if column_index is not None and column_index in self.columns.keys():
            set_index = column_index
        elif column_name is not None:
            test_index = self.return_column_index_by_name(column_name)
            if test_index is not None:
                set_index = test_index
                
        if set_index is None and column_index is not None:
            raise ValueError(f"Column index {column_index} not found in list of known column indexes")
        elif set_index is None and column_name is not None:
            raise ValueError(f"Column name {column_name} not found in list of known column names")
        
        indexes_to_remove = []
        for idx, row in self.rows.items():
            items = row.items
            test_value = items[set_index]
            if test_value == value:
                indexes_to_remove.append(idx)
                
        for index in indexes_to_remove:
            self.rows.pop(index)
            
        self.row_count -= len(indexes_to_remove)
            
        self.reset_row_indexes()
    
    def remove_row_by_index(self, index):
        if index in self.rows.keys():
            self.rows.pop(index)
            self.row_count -= 1
    
    def return_row_items_by_index(self, index):
        if index in self.rows.keys():
            row_object = self.rows[index]
            return row_object.items
        raise ValueError(f"Row index {index} not found in list of known row indexes")

    def return_sub_row_by_index(self, row_index, column_indexes = [], column_names = []):
        if row_index not in self.rows.keys():
            raise ValueError(f"Row index {row_index} not found in list of row indexes")
        
        values = self.rows[row_index].items
        found_column_indexes = []
        
        if column_indexes != []:
            for idx in column_indexes:
                if idx in self.columns.keys():
                    found_column_indexes.append(idx)
                    
        elif column_names != []:
            for name in column_names:
                idx = self.return_column_index_by_name(name)
                if idx is not None:
                    found_column_indexes.append(idx)
        
        sub_row = []
        for index in found_column_indexes:
            sub_row.append(values[index])
            
        return sub_row

    def return_distinct_column_values(self, column_index = None, column_name = None, include_nulls = False, sort = True):
        found_column_index = self.check_and_return_column_index(column_index = column_index, column_name = column_name)
        if found_column_index is not None:
            distinct_values = []
            values = self.return_column_data(found_column_index)
            for value in values:
                if value not in distinct_values:
                    if include_nulls == True:
                        distinct_values.append(value)
                    elif include_nulls == False and value != "NULL":
                        distinct_values.append(value)                    
            if sort == True:
                return sorted(distinct_values)
            else:
                return distinct_values

    def order_rows_by_column(self, column_index = None, column_name = None, mode = "asc"):
        index = self.check_and_return_column_index(column_index = column_index, column_name = column_name)
        if index is None:
            raise ValueError(f"Column name or index not recognised in list of known columns")
        
        distinct_values = self.return_distinct_column_values(column_index=index)    
        
        if mode == "desc":
            distinct_values.reverse()
        
        distinct_values.append("NULL")    
        
        row_dict = self.rows
        rows_length = len(row_dict)
        
        counter = 0
        new_dict = {}
        
        pop_indexes = []
        
        while rows_length != len(new_dict):      
            for value in distinct_values:
                for idx, row in row_dict.items():
                    items = row.items
                    row_value = items[index]
                    if row_value == value:
                        new_dict[counter] = row
                        pop_indexes.append(idx)
                        counter += 1
                for idx in pop_indexes:
                    row_dict.pop(idx)
                pop_indexes = []
                        
        self.rows = {}
        self.rows = new_dict      

    def return_rows_as_lists(self):
        return_list = []
        for row in self.rows.values():
            return_list.append(row.items)
        return return_list

    ### Indexes
    def reset_row_indexes(self):
        old_rows = self.rows
        index = 0
        new_rows = {}
        for row in old_rows.values():
            new_rows[index] = row
            index += 1
        self.rows = new_rows
        self.last_row_index = index
        
    def reset_column_indexes(self):
        items = []
        for key, column in self.columns.items():
            items.append(column)
        self.columns = {}
        index = 0
        for item in items:
            item.set_index(index)
            self.column_count = 0
            self._internal_add_column(item)
            index += 1   
        self.column_count = len(self.columns) 
   
    def check_and_return_column_index(self, column_index = None, column_name = None):
        index = None
        if column_index is not None:
            if column_index not in self.columns.keys():
                raise ValueError(f"Column index {column_index} not found in list of known column indexes")   
            index = column_index
        elif column_name is not None:
            test_index = self.return_column_index_by_name(column_name)
            if test_index not in self.columns.keys():
                raise ValueError(f"Column name {column_name} not found in list of known column names")
            index = test_index
        return index
        
    ### Selection   
                       
    def top(self, number):
        print(f"\nPrinting top {number} rows")
        columns = self.column_names()        
        print_rows = []
        for idx, row in self.rows.items():
            if idx < number:
                print_rows.append(row.items)       
        print(columns) 
        print(len(columns) * "-")
        
        self._internal_print_items(print_rows)  
    
    def bottom(self, number):
        print(f"\nPrinting bottom {number} rows")
        columns = self.column_names()        
        print_rows = []
        iteration = 0
        for idx, row in reversed(self.rows.items()):
            if iteration != number:
                print_rows.append(row.items)      
                iteration += 1 
        print(columns)  
        print(len(columns) * "-")
        self._internal_print_items(print_rows)  
    
    def random_selection(self, number):
        print(f"\nPrinting {number} random rows")
        columns = self.column_names()                
        table_row_count = self.row_count
        
        if table_row_count < number:
            for row in self.rows.values():
                string_list = [str(item) for item in row]
                print(" | ".join(string_list))
            return
        
        print_rows = []        
        available_indexes = list(self.rows.keys())        
           
        for iteration in range(number):
            index_range = len(available_indexes)
            random_index = random.randint(0,index_range - 1)
            print_rows.append(self.rows[random_index].items)
            available_indexes.pop(random_index)    
      
        print(columns)  
        print(len(columns) * "-")
        self._internal_print_items(print_rows)    
    
    def print(self):
        print(f"\nPrinting all rows")
        columns = self.column_names()        
    
        print(columns)  
        print(len(columns) * "-")
        
        print_items = [row.items for row in self.rows.values()]        
        self._internal_print_items(print_items)
    
    def _internal_print_items(self, print_rows):
        for print_row in print_rows:
            string_list = []
            for idx, column in self.columns.items():
                value = print_row[idx]
                padded = self._internal_modify_string_to_whitespace_padding(value, idx)
                string_list.append(padded)               
            
            print(" | ".join(string_list))
        return print_rows   
    
    def _internal_modify_string_to_whitespace_padding(self, input_value, index):
        length_dict = self._internal_get_length_dict()
        input_string = str(input_value)
        input_length = len(input_string)
        total_space = length_dict[index]
        if input_length < total_space:
            padding = total_space - input_length
            return " " * int(padding / 2) + input_string + " " * int(padding / 2)
        else:
            return input_string
    
    def _internal_get_length_dict(self):
        spacing_dict = {}
        for idx, column in self.columns.items():
            length = len(column.name)
            spacing_dict[idx] = length
        return spacing_dict
    
    ### Calculations
    def return_max_value(self, column_index = None, column_name = None):
        return self._internal_return_min_max_value("max", column_index = column_index, column_name = column_name)        
        
    def return_min_value(self, column_index = None, column_name = None):
        return self._internal_return_min_max_value("min", column_index = column_index, column_name = column_name)
    
    def _internal_return_min_max_value(self, mode, column_index = None, column_name = None):
        if column_index is not None:
            if column_index not in self.columns.keys():
                raise ValueError(f"Column index {column_index} not found in list of known column indexes")   
            index = column_index
        elif column_name is not None:
            test_index = self.return_column_index_by_name(column_name)
            if test_index not in self.columns.keys():
                raise ValueError(f"Column name {column_name} not found in list of known column names")
            index = test_index
        column_object:Column = self.columns[index]
        data_type = column_object.data_type
        if data_type != "NULL":
            
            last_max = None
            last_sub = None
            
            last_min = None
            
            true_count = 0
            false_count = 0
            
            values = self.return_column_data(column_index=index)            
            for value in values:
                #print(f"Comparing {value} to {last_max}")
                this_value = None
                if value != "NULL":
                    if data_type == "string": this_value = len(value)
                    elif data_type == "int": this_value = value
                    elif data_type == "decimal": this_value = value
                    elif data_type == "bool":
                        if value == True: true_count += 1
                        elif value == False: false_count += 1
                    elif data_type == "date" or data_type == "datetime":
                        this_value = value
                        
                    if this_value is not None and last_max is None and mode == "max": 
                        last_max = this_value
                        if data_type == "string": last_sub = value
                    if this_value is not None and last_min is None and mode == "min": 
                        last_min = this_value
                        if data_type == "string": last_sub = value
                    elif this_value is not None and last_max is not None:
                        if this_value > last_max and mode == "max": 
                            last_max = this_value
                            if data_type == "string": last_sub = value
                    elif this_value is not None and last_min is not None:
                        if this_value < last_min and mode == "min":
                            last_min = this_value
                            if data_type == "string": last_sub = value
                    
            if data_type == "bool" and mode == "max":
                if true_count > false_count: return True
                elif true_count < false_count: return False        
                else: return None
            elif data_type == "bool" and mode == "min":
                if true_count < false_count: return True
                elif true_count > false_count: return False        
                else: return None
                
            if data_type == "string": return last_sub
            
            if mode == "max":
                return last_max
            elif mode == "min":
                return last_min 
   
    def return_range(self, column_index = None, column_name = None):
        min = self.return_min_value(column_index=column_index, column_name=column_name)
        max = self.return_max_value(column_index=column_index, column_name=column_name)
        
        data_type = type(min)
        
        if data_type is int or data_type is float:
            return max - min    
        
        if data_type is str:
            min_chars = len(min)
            max_chars = len(max)
            range_chars = max_chars - min_chars
            return range_chars
        
        if data_type is datetime:
            difference:timedelta = max - min
            return difference
        
        raise ValueError(f"Data type of column data not compatible with use of range. Must be int, decimal, string, date or datetime")
   
    def return_mode(self, column_index = None, column_name = None):
        index = self.check_and_return_column_index(column_index, column_name)
        if index is not None:
            value_dictionary = {}
            values = self.return_column_data(index)
            for value in values:
                if value not in value_dictionary:
                    value_dictionary[value] = 1
                else:
                    current_number = value_dictionary[value]
                    value_dictionary[value] = current_number + 1
                    
        highest_occurence = 0
        highest_value = None
        for key, val in value_dictionary.items():
            if val > highest_occurence:
                highest_value = key
                highest_occurence = val
        return highest_value, highest_occurence
    
    def return_mean(self, column_index = None, column_name = None):
        index = self.check_and_return_column_index(column_index, column_name)
        if index is not None:
            values = self.return_column_data(index)
            values_only = [value for value in values if value != "NULL"]
            
            dtype = type(values_only[0])
            column_type = self.columns[index].data_type
            
            if dtype is int or dtype is float:
                total = sum(values_only)
                return total / len(values_only)     
            elif dtype is str:
                string_length_list = [len(value) for value in values_only]
                total = sum(string_length_list)
                return string_length_list / len(values_only)
            elif dtype is bool:
                raise ValueError("Cannot return the mean value of a bool column")
            elif column_type == "date" or column_type == "datetime":
                raise ValueError("Cannot return the mean value of a date or datetime")    
    
    def return_sum(self, column_index = None, column_name = None):
        index = self.check_and_return_column_index(column_index, column_name)
        if index is not None:
            values = self.return_column_data(index)
            values_only = [value for value in values if value != "NULL"]
            
            dtype = type(values_only[0])
            column_type = self.columns[index].data_type
            
            if dtype is int or dtype is float:
                total = sum(values_only)
                return total
            else:
                return None      
        
    def return_median(self, column_index = None, column_name = None):
        index = self.check_and_return_column_index(column_index, column_name)
        if index is not None:
            values = self.return_column_data(index)
            values_only = [value for value in values if value != "NULL"]
            mid_point = len(values_only) // 2
            
            dtype = type(values_only[0])
            column_type = self.columns[index].data_type
            
            if dtype is int or dtype is float:
                sorted_list = sorted(values_only) 
            elif dtype is str:
                string_length_list = [len(value) for value in values_only]
                sorted_list = sorted(string_length_list)      
            elif column_type == "date" or column_type == "datetime":
                sorted_list = sorted(values_only)
                
            if len(sorted_list) % 2 == 0:
                value_one = sorted_list[mid_point - 1]
                value_two = sorted_list[mid_point]
                
                if dtype is int or dtype is float or dtype is str:
                    return ( value_one + value_two ) / 2
                elif column_type == "date" or column_type == "datetime":
                    datetime_difference = value_two - value_one
                    return value_two - (datetime_difference / 2)
            else:
                return sorted_list[mid_point]
                    
            if dtype is bool:
                raise ValueError("Cannot return the mean value of a bool column")
        
    def return_null_count(self, column_index = None, column_name = None):
        index = self.check_and_return_column_index(column_index, column_name)
        if index is not None:
            values = self.return_column_data(index)
            nulls = [value for value in values if value == "NULL"]
            return len(nulls)
        
    def return_non_null_count(self, column_index = None, column_name = None):
        index = self.check_and_return_column_index(column_index, column_name)
        if index is not None:
            values = self.return_column_data(index)
            non_nulls = [value for value in values if value != "NULL"]
            return len(non_nulls)
          
    def return_variance(self, column_index = None, column_name = None):
        found_column_index = self.check_and_return_column_index(column_index = column_index, column_name = column_name)
        column_object:Column = self.columns[found_column_index]
        data_type = column_object.data_type
        
        if data_type == "int" or data_type == "decimal":
            values = self.return_column_data(found_column_index, include_nulls=False)
            
            mean_value = self.return_mean(found_column_index)
            variances = []

            for value in values:
                distance = mean_value - value
                variance = distance ** 2
                variances.append(variance)
            return sum(variances) / len(variances)
                    
        else:
            return ValueError(f"Column at index {found_column_index} is not an int or a decimal")                
        
    def return_standard_deviation(self, column_index = None, column_name = None):
        found_column_index = self.check_and_return_column_index(column_index = column_index, column_name = column_name)
        variance = self.return_variance(found_column_index)
        return variance ** 0.5          
  
    def add_calculation_column(self, reference_column_index = None, reference_column_name = None, calculation = None, calculation_value = None):
        options = ["ntile", "rank", "individual_std", "individual_variance", "row_number", "+ days", "- days", "above_threshold_percent", "below_threshold_percent"]
        
        if calculation not in options:
            raise KeyError(f"Input option {calculation} not in list of approved options: {options}")
        
        column_object = None
        
        if reference_column_index is not None:
            if reference_column_index not in self.columns.keys():
                raise ValueError(f"Input index {reference_column_index} not in list of known column indexes")
            else:
                column_object = self.columns[reference_column_index]
        
        for idx, column in self.columns.items():
            name = column.name            
            if name == reference_column_name:
                column_object = column
                break
        
        if column_object is None:            
            raise ValueError(f"Could not find column by name {reference_column_name} in list of known column names")
        
        reference_data_type = column_object.data_type
        reference_index = column_object.index
        
        index = self.column_count
        if index in self.columns:
            raise KeyError(f"Error. Expected index {index} is already in use. Reset column indexes first")                    
        
        new_column = Column("new_calculated_column", index)     
        
        if calculation == "ntile":
            if calculation_value is None or type(calculation_value) is not int:
                raise ValueError(f"Ntile calculations require a calculation value in the form of an integer")
            if reference_data_type not in ["int", "decimal", "date","datetime"]:
                raise ValueError(f"Ntile calculations require a reference column of int, decimal, date or datetime")
            new_column.set_name(f"calculated_ntile_{calculation_value}")    
            new_column.set_data_type("int")    
            
            self.order_rows_by_column(reference_index)            
            
            row_count = self.row_count
            buckets = calculation_value
            steps = math.floor(row_count / buckets)
            
            bucket_list = range(1, buckets + 1)
            
            idx1 = 0
            idx2 = math.floor(row_count / buckets)
            
            values_dict = {}
            row_list = [[idx, row] for idx,row in self.rows.items()]
                        
            for bucket in bucket_list:
                if bucket == bucket_list[-1]:
                    values_dict[bucket] = row_list[idx1 : ]
                else:
                    values_dict[bucket] = row_list[idx1 : idx2]
                    idx1 = idx2
                    idx2 += steps
                    
            for bucket_number, bucket_values in values_dict.items():
                for row in bucket_values:
                    row_idx = row[0]
                    row_items = row[1].items
                    row_items.append(bucket_number)
                    self.update_row(row_idx, row_items)
                
        elif calculation == "rank":
            if reference_data_type not in ["int", "decimal", "date","datetime"]:
                raise ValueError(f"Rank calculations require a reference column of int, decimal, date or datetime")
            new_column.set_name("calculated_rank")
            new_column.set_data_type("int")               
            
            self.order_rows_by_column(reference_index)
            
            counter = 1
            for idx, row in self.rows.items():
                items = row.items
                items.append(counter)
                self.update_row(idx, items)
                counter += 1
                         
        elif calculation == "individual_std":
            if reference_data_type not in ["int", "decimal"]:
                raise ValueError(f"Individual STD calculations require a reference column of int or decimal")
            new_column.set_name("calculated_individual_std")
            new_column.set_data_type("decimal")    
            
            mean = self.return_mean(reference_index)
            
            for idx, row in self.rows.items():
                row_items = row.items
                row_value = row_items[reference_index]                
                if row_value != "NULL":                    
                    variance_value = (mean - row_value) ** 2
                    std = variance_value ** 0.5
                    row_items.append(std)
                    self.update_row(idx, row_items)
                else:
                    row_items.append("NULL")
                    self.update_row(idx, row_items)
                
        elif calculation == "individual_variance":
            if reference_data_type not in ["int", "decimal"]:
                raise ValueError(f"Individual variance calculations require a reference column of int or decimal")
            new_column.set_name("calculated_individual_var")
            new_column.set_data_type("decimal")        
            
            mean = self.return_mean(reference_index)
            
            for idx, row in self.rows.items():
                row_items = row.items
                row_value = row_items[reference_index]
                if row_value != "NULL":                    
                    variance_value = (mean - row_value) ** 2
                    row_items.append(variance_value)
                    self.update_row(idx, row_items)
                else:
                    row_items.append("NULL")
                    self.update_row(idx, row_items)
            
        elif calculation == "row_number":
            new_column.set_name("calculated_row_number")
            new_column.set_data_type("int")       
            
            counter = 1
            for idx, row in self.rows.items():
                items = row.items
                items.append(counter)
                self.update_row(idx, items)
                counter += 1
             
        elif calculation == "+ days":
            if calculation_value is None or type(calculation_value) is not int:
                raise ValueError(f"Date + calculations require a calculation value in the form of an integer")
            if reference_data_type not in ["date","datetime"]:
                raise ValueError(f"Date calculations require a reference column of date or datetime")
            new_column.set_name("calculated_+_days")
            new_column.set_data_type("date")    
            
            for idx, row in self.rows.items():
                items = row.items
                reference_value = items[reference_index]  
                if reference_value != "NULL":
                    additional_days = timedelta(days=calculation_value)
                    new_date = reference_value + additional_days
                    items.append(new_date)
                    self.update_row(idx, items)
                else:
                    items.append("NULL")
                    self.update_row(idx, items)
                
        elif calculation == "- days":
            if calculation_value is None or type(calculation_value) is not int:
                raise ValueError(f"Date - calculations require a calculation value in the form of an integer")
            if reference_data_type not in ["date","datetime"]:
                raise ValueError(f"Date calculations require a reference column of date or datetime")
            new_column.set_name("calculated_-_days")
            new_column.set_data_type("date")  
            
            for idx, row in self.rows.items():
                items = row.items
                reference_value = items[reference_index]  
            if reference_value != "NULL":
                removal_days = timedelta(days=calculation_value)
                new_date = reference_value - removal_days
                items.append(new_date)
                self.update_row(idx, items)  
            else:
                items.append("NULL")
                self.update_row(idx, items)  
                
        elif calculation == "above_threshold_percent":
            if calculation_value is None or (type(calculation_value) is not int and type(calculation_value) is not float) :
                raise ValueError(f"Threshold calculations require a calculation value in the form of an integer")
            if reference_data_type not in ["int", "decimal", "date","datetime"]:
                raise ValueError(f"Threshold calculations require a reference column of int, decimal, date or datetime")
            new_column.set_name("calculated_above_theshold")
            new_column.set_data_type("bool")  
            
            max = self.return_max_value(reference_index)
            min = self.return_min_value(reference_index)
            difference = max - min
            
            threshold = difference * (calculation_value / 100)

            new_column.set_name(f"calculated_above_theshold_{calculation_value}_percent")            
            
            for idx, row in self.rows.items():
                items = row.items
                value = items[reference_index]
                if value != "NULL":
                    threshold_bool = None
                    if reference_data_type in ["int", "decimal"]:
                        if value > threshold:
                            threshold_bool = True
                        else: threshold_bool = False
                    elif reference_data_type in ["date", "datetime"]:
                        if value > (min + threshold):
                            threshold_bool = True
                        else: threshold_bool = False
                    items.append(threshold_bool)
                    self.update_row(idx, items)              
                else:
                    items.append("NULL")
                    self.update_row(idx, items)     
              
        elif calculation == "below_threshold_percent":
            if calculation_value is None or type(calculation_value) is not int:
                raise ValueError(f"Threshold calculations require a calculation value in the form of an integer")
            if reference_data_type not in ["int", "decimal", "date","datetime"]:
                raise ValueError(f"Threshold calculations require a reference column of int, decimal, date or datetime")
            new_column.set_name("calculated_below_theshold")
            new_column.set_data_type("bool")     
            
            max = self.return_max_value(reference_index)
            min = self.return_min_value(reference_index)
            difference = max - min
            
            threshold = difference * (calculation_value / 100)

            new_column.set_name(f"calculated_above_theshold_{calculation_value}_percent")            
            
            for idx, row in self.rows.items():
                items = row.items
                value = items[reference_index]
                if value != "NULL":
                    threshold_bool = None
                    if reference_data_type in ["int", "decimal"]:
                        if value < threshold:
                            threshold_bool = True
                        else: threshold_bool = False
                    elif reference_data_type in ["date", "datetime"]:
                        if value < (min + threshold):
                            threshold_bool = True
                        else: threshold_bool = False
                    items.append(threshold_bool)
                    self.update_row(idx, items)        
                else:
                    items.append("NULL")
                    self.update_row(idx, items)  
            
        self._internal_add_column(new_column)
  
    ### Duplicates
    def remove_duplicate_rows(self):
        if self.column_count == 0:
            raise IndexError("There are no columns in the cyber table")               
        if self.row_count == 0:
            raise ValueError("There are no rows in the cyber table")
        distinct_column_zero_values = self.return_distinct_column_values(0, include_nulls=True, sort = False)
        
        indexes_to_remove = []
        
        row_dict = {}
        for value in distinct_column_zero_values:
            row_dict[value] = []
        row_dict["NULL"] = []       
                    
        for idx, row in self.rows.items():
            items = row.items
            item_one = items[0]
            
            if items not in row_dict[item_one]:
                row_dict[item_one].append(items)
            else:
                indexes_to_remove.append(idx)               
                
        for index in indexes_to_remove:
            self.rows.pop(index)
            self.row_count -= 1
        
        self.reset_row_indexes()
        
        return len(indexes_to_remove), indexes_to_remove
        
    def remove_duplicate_rows_by_columns(self, column_indexes = [], column_names = []):        
        sub_table = self.return_sub_table_by_columns(column_indexes = column_indexes, column_names = column_names)
        
        sub_table.reset_column_indexes()
        sub_table.print_structure()

        removals, index_list = sub_table.remove_duplicate_rows()

        for index in index_list:
            self.rows.pop(index)
            self.row_count -= 1
        
        self.reset_row_indexes()
        
        return removals, index_list   
  
    ### Sub Tables             
    def return_sub_table_by_columns(self, column_indexes = [], column_names = []):
        found_indexes = []
        if column_indexes != []:
            for index in column_indexes:
                if index in self.columns:
                    found_indexes.append(index)
        elif column_names != []:
            for name in column_names:
                idx = self.return_column_index_by_name(name)
                if idx is not None:
                    found_indexes.append(idx)
                    
        if found_indexes == 0:
            raise ValueError(f"No column indexes found from inputs")
            
        new_cyber_table = CyberTable()   
        
        for column_index in found_indexes:
            column_object = self.columns[column_index]
            new_cyber_table._internal_add_column(column_object)
        
        for index in self.rows.keys():           
            sub_row = self.return_sub_row_by_index(index, found_indexes)  
            new_cyber_table.add_row(sub_row)         
        
        return new_cyber_table                 
            
    def return_sub_table_by_row_filters(self, values:list , column_indexes = [], column_names = []):
        if values == []:
            raise ValueError(f"Number of input items is empty")
        
        found_indexes = []
        if column_indexes != []:
            for idx in column_indexes:
                if idx in self.columns.keys():
                    found_indexes.append(idx)
                    
        elif column_names != []:
            for name in column_names:
                idx = self.return_column_index_by_name(name)
                if idx is not None:
                    found_indexes.append(idx)
                    
        if len(found_indexes) != len(values):
            raise ValueError(f"Number of found column indexes does not match the number of values inputted")
        
        #print(f"Starting recurrsion!!!")
        filtered_rows = self._internal_return_rows_by_value_recursive(self.rows, found_indexes, values)
        #print(f"Recurrsion finished. Rows: {len(filtered_rows)}")
        
        new_cyber_table = CyberTable()
        
        for column in self.columns.values():                   
            new_cyber_table._internal_add_column(column)
        
        for idx, row_object in filtered_rows.items():           
            new_cyber_table._internal_add_row(row_object, idx)
            
        new_cyber_table.reset_row_indexes()
            
        return new_cyber_table        
        
    ### String Functions    
    def clean_string_column(self, column_index = None, column_name = None, capital_first_letter = True):
        found_column_index = self.check_and_return_column_index(column_index = column_index, column_name = column_name)
        column = self.columns[found_column_index]
        name = column.name
        
        if column.data_type != "string":
            raise TypeError(f"Column {name} is not a string")
        
        for row_idx, row in self.rows.items():
            string_value:str = row.items[found_column_index]
            string_value = string_value.strip()
            
            if capital_first_letter == True:
                first_letter:str = string_value[0]
                first_letter = first_letter.upper()
                string_value = first_letter + string_value[1:]
                
            items = row.items
            items[found_column_index] = string_value
            
            self.rows[row_idx].set_items(items)        
        
    def set_column_string_case(self, case, column_index = None, column_name = None):
        found_column_index = self.check_and_return_column_index(column_index = column_index, column_name = column_name)
        column = self.columns[found_column_index]
        name = column.name
        
        if column.data_type != "string":
            raise TypeError(f"Column {name} is not a string")
        
        column_data = self.return_column_data(found_column_index)
        
        if case == "lower":
            new_column_data = [str(value).lower() for value in column_data]
        elif case == "lower_snake":
            new_column_data = [str(value).lower().replace(" ", "_") for value in column_data]
        elif case == "upper":
            new_column_data = [str(value).upper() for value in column_data]
        elif case == "upper_snake":
            new_column_data = [str(value).upper().replace(" ", "_") for value in column_data]
        elif case == "title":
            new_column_data = [convert_string_to_title_case(value) for value in column_data]
        elif case == "title_snake":
            new_column_data = [convert_string_to_title_case(value).replace(" ", "_") for value in column_data]
            
        for idx, row in self.rows.items():
            items = row.items
            items[found_column_index] = new_column_data[0]
            new_column_data.pop(0)
            self.rows[idx].set_items(items)       
        
    def convert_iso_8601_string_to_datetime(self, column_index = None, column_name = None):
        index = self.check_and_return_column_index(column_index = column_index, column_name = column_name)    
        column = self.columns[index]
        
        if column.data_type != "string":
            raise ValueError(f"Input must be a string data type")
        
        update_dict = {}
        
        for idx, row in self.rows.items():
            items = row.items
            value = items[index]
            if value != "NULL":
                new_value = convert_iso_8601_to_datetime(value)
                items[index] = new_value
                update_dict[idx] = items
        
        for idx, items in update_dict.items():
            self.update_row(idx, items)

        self.analyse_columns(index)
        
    ### Misc    
    def return_copy(self):
        new_table = CyberTable()
        for idx, row in self.rows.items():
            new_table._internal_add_row(row, idx)
        
        for idx, column in self.columns.items():
            data = self.return_column_data(idx)  
            new_table._internal_add_column(column)      
             
        return new_table
    
    def save_as_csv(self, directory, file, delimiter = ","):
        if os.path.isdir(directory) == False:
            raise ValueError(f"Directory {directory} does not exist")
        target_file_path = directory + "\\" + file + ".csv"
        
        if os.path.exists(target_file_path):
            raise ValueError(f"The target file: {target_file_path} already exists")
        
        columns = self.columns.values()
        name_list = [column.name for column in columns]
        
        with open(target_file_path, "a") as writer:        
            writer.write(delimiter.join(name_list) + "\n")
            for row in self.rows.values():
                items = [str(item) for item in row.items]
                writer.write(delimiter.join(items))
                writer.write("\n")
    
    ### Grouping
    def return_groups(self, column_indexes = [], column_names = []):
        approved_indexes = []
        
        if column_indexes != []:
            for index in column_indexes:
                if index not in self.columns:
                    raise ValueError(f"Index {index} not in list of known column indexes")
                approved_indexes.append(index)
        
        elif column_names != []:
            for name in column_names:
                index = self.return_column_index_by_name(name)
                if index is None:     
                    raise ValueError(f"Column name {name} not in list of known column names")
                approved_indexes.append(index)                
        
        copy = self.return_copy()
        sub_table_list = self._internal_return_sub_table_groups_recursive([copy], approved_indexes)      
        
        table_group = CyberTableGroup()
        for table in sub_table_list:
            table.row_count = len(table.rows)
            table.reset_row_indexes()
            table_group.add_table(table, approved_indexes)
            
        return table_group
       
    def _internal_return_sub_table_groups_recursive(self, tables:list, index_list):
        
        if len(index_list) == 0 or index_list is None:
            #wait(f"Base case reached. Returning {len(tables)} tables")
            return tables
        
        new_table_list = []
        column_index = index_list[0]    
        
        for table in tables:            
            distinct_values = table.return_distinct_column_values(column_index)
            for value in distinct_values:
                new_table = table.return_sub_table_by_row_filters(values=[value], column_indexes = [column_index])
                new_table_list.append(new_table)
                
        if len(index_list) == 1:
            new_index_list = []
        else:
            new_index_list = index_list[1:]
            
        return self._internal_return_sub_table_groups_recursive(new_table_list, new_index_list)
             
    def aggregate(self, reference_column_indexes = [], reference_column_names = [], calculation_column_indexes = [], calculation_column_names = [], calculations = []):
       
        column_index_list = []
        calculation_column_index_list = []
        
        if reference_column_indexes != []:
            for index in reference_column_indexes:
                if index not in self.columns.keys():
                    raise ValueError(f"Input index {index} not in list of known column indexes")
                else:
                    column_index_list.append(index)
                
        elif reference_column_names != []:
            for reference_name in reference_column_names:
                found = False
                for idx, column in self.columns.items():
                    name = column.name            
                    if name == reference_name:
                        found = True
                        column_index_list.append(idx)
                        break
                if found == False:
                    raise KeyError(f"Column name {reference_name} not found in list of known column names")
                
        if calculation_column_indexes != []:
            for index in calculation_column_indexes:
                if index not in self.columns.keys():
                    raise ValueError(f"Input index {index} not in list of known column indexes")
                else:
                    calculation_column_index_list.append(index)
                
        elif calculation_column_names != []:
            for calculation_name in calculation_column_names:
                found = False
                for idx, column in self.columns.items():
                    name = column.name            
                    if name == calculation_name:
                        found = True
                        calculation_column_index_list.append(idx)
                        break
                if found == False:
                    raise KeyError(f"Column name {calculation_name} not found in list of known column names")
               
        if len(calculation_column_index_list) != len(calculations):
            raise KeyError(f"Count of columns {len(column_index_list)} does not match the number of calculations {len(calculations)}")        
        for index in column_index_list:
            if index in calculation_column_index_list:
                raise KeyError(f"Reference columns cannot also be calculation columns, they bust be distinctly separate")        
      
        groups = self.return_groups(column_index_list)
        aggregate_table = groups.aggregate(reference_column_indexes=column_index_list, calculation_column_indexes=calculation_column_index_list, calculations=calculations)
        return aggregate_table       
       
# Normal Functions    
def wait(message = None):
    if message == None:
        input("waiting...")
    else:
        input(message)
    
def convert_string_to_title_case(input):
    return_chars = []
    string_input = str(input)
    last_char = None
    for char in string_input:        
        if last_char == None:
            return_chars.append(str(char).upper())
            last_char = char
        elif last_char == " ":
            return_chars.append(str(char).upper())
            last_char = char
        else:
            return_chars.append(str(char).lower())
            last_char = char
    return "".join(return_chars)
    
def is_int(input):
    if "." in str(input):
        return False
    try:
        int(input)
        return True
    except:
        return False
    
def is_decimal(input):
    if "." in str(input):
        try:
            float(input)
            return True
        except:
            return False

def is_bool(input):
    bools = ["true", "false"]
    if str(input).lower() in bools:
        return True
    return False

def string_to_bool(input):
    if is_bool(input) == True:
        lower = str(input).lower()
        if lower == "true": return True
        elif lower == "false": return False

def is_date(input):
    input_string = str(input)
    try:
        date_value = datetime.strptime(input_string, "%Y-%m-%d")
        return True
    except:
        return False

def is_datetime(input):
    input_string = str(input)
    try:
        datetime_value = datetime.strptime(input_string, "%Y-%m-%d %H:%M:%S")
        return True
    except:
        return False
        
def replace_missing_with_nulls(input_list:list):
        return_list = []
        for item in input_list:
            if len(str(item)) == 0:
                return_list.append("NULL")
            else: return_list.append(item)
        return return_list
    
def convert_iso_8601_to_datetime(input):
    input_string = str(input)
    if "Z" not in input_string or "T" not in input_string:
        raise ValueError(f"Input: {input} doesn't appear to be ISO 8601")
    input_string = input_string.replace("T", " ").replace("Z", "")
    try:
        new_value = datetime.strptime(input_string, "%Y-%m-%d %H:%M:%S")
        return new_value
    except:
        raise ValueError(f"Input: {input_string} is not a datetime in format yyyy-mm-dd hh:mm:ss")
    
def open_csv(file, delimiter = ",") -> CyberTable:
    
    if os.path.exists(file):
        # Create the table object
        cyber_table = CyberTable()
        
        # Open the file and read the lines using the delimiter
        with open(file, "r") as open_file:
            lines = open_file.readlines()           
            column_line = lines[0].rstrip("\n") 
            columns = column_line.split(delimiter)

            # Add column objects to the table
            index = 0
            for column in columns:                
                column_object = Column(column, index)
                cyber_table._internal_add_column(column_object)
                index += 1
            
            # Add all lines to the table object
            for line in lines[1:]:
                line_string = line.rstrip("\n")
                line_string = line_string.replace(",,", ",NULL,")
                line_list = line_string.split(delimiter) 
                cyber_table.add_row(line_list)
                
        # Detect data types
        cyber_table.analyse_columns()    
                
        # Return the table object
        return cyber_table
    else:
        return None
    


