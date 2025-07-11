import os
import random
import math
from datetime import datetime, timedelta
from datetime import time as dtime

calculation_column_options = ["ntile", "rank", "individual_std", "individual_variance", "row_number", "+ days", "- days", "days_between", "above_threshold_percent", "below_threshold_percent"]
aggregation_options = ["sum", "mean", "mode", "median", "max", "min", "nulls", "non_nulls", "row_counts", "standard_deviation", "variance", "range", "true_percentage", "false_percentage"]      
print_data_overview_options = ["all", "numeric", "string", "bool", "date"]

class Column():
    def __init__(self, name, index, data_type = None):
        self.name = name.replace("\n", "").strip()
        self.data_type = data_type
        self.index = index    
        self.allow_analyse = True
        self.longest_value = 0
    def set_index(self, index):
        self.index = index
    def get_index(self):
        return self.index
    def set_data_type(self, data_type):
        self.data_type = data_type
    def get_data_type(self):
        return self.data_type
    def set_name(self, name):
        self.name = name
    def get_name(self):
        return self.name
    def lock_data_type(self):
        self.allow_analyse = False
    def unlock_data_type(self):
        self.allow_analyse = True
    def get_analyse_property(self):
        return self.allow_analyse
    def set_longest_value(self, value):
        self.longest_value = value
    def get_longest_value(self):
        return self.longest_value
    def print(self):
        print(f"Index: {self.get_index()}, Name: {self.get_name()}, Data Type: {self.get_data_type()}, Unlocked: {self.get_analyse_property()}, Longest value: {self.get_longest_value()}")
         
    # Accepted data types = int, decimal, bool, date, datetime, string, null
        
class Row():
    def __init__(self, index, items):
        self.index = index
        self.items = items     
    def set_index(self, index):
        self.index = index
    def get_index(self):
        return self.index
    def set_items(self, items):
        self.items = items
    def get_items(self):
        return self.items  
    def print(self):
        print(f"Index: {self.get_index()}, Items: {self.get_items()}")
            
class CyberTable():
    def __init__(self):
        self.columns = {}
        self.rows = {}
        self.column_count = 0
        self.row_count = 0
        self.last_row_index = None
        
    ### Comparrison dunder methods
    def __lt__(self, other):
        if type(other) is CyberTable:
            this_count = self.return_row_count()
            other_count = other.return_row_count()
            if this_count < other_count: return True
            else: return False
            
    def __le__(self, other):
        if type(other) is CyberTable:
            this_count = self.return_row_count()
            other_count = other.return_row_count()
            if this_count <= other_count: return True
            else: return False
            
    def __gt__(self, other):
        if type(other) is CyberTable:
            this_count = self.return_row_count()
            other_count = other.return_row_count()
            if this_count > other_count: return True
            else: return False
            
    def __ge__(self, other):
        if type(other) is CyberTable:
            this_count = self.return_row_count()
            other_count = other.return_row_count()
            if this_count >= other_count: return True
            else: return False
            
    def __eq__(self, other):
        if type(other) is CyberTable:
            this_count = self.return_row_count()
            other_count = other.return_row_count()
            if this_count == other_count:                
                for idx in self.rows.keys():
                    row_match = None
                    self_items = self.return_row_items_by_index(idx)
                    other_items = self.return_row_items_by_index(idx)
                    item_count = len(self_items)
                    for item in range(item_count):
                        self_item = self_items[item]
                        other_item = other_items[item]
                        if self_item == other_item:
                            row_match = True
                        else:
                            row_match = False
                            
                        if row_match == False:
                            return False                
                return True                    
            else: return False
            
    def __ne__(self, other):
        if type(other) is CyberTable:
            this_count = self.return_row_count()
            other_count = other.return_row_count()
            if this_count == other_count:                
                for idx in self.rows.keys():
                    row_match = None
                    self_items = self.return_row_items_by_index(idx)
                    other_items = self.return_row_items_by_index(idx)
                    item_count = len(self_items)
                    for item in range(item_count):
                        self_item = self_items[item]
                        other_item = other_items[item]
                        if self_item == other_item:
                            row_match = True
                        else:
                            row_match = False
                            
                        if row_match == False:
                            return True                
                return False                    
            else: return True
           
    ### Arithmatic dunder methods
    
    def __add__(self, other):
        if type(other) == CyberTable:
            group = CyberTableGroup()
            group.add_table(self)
            group.add_table(other)
            return group.merge_into_cyber_table()
        else:
            raise TypeError(f"Can only add to CyberTable objects together")
        
    def __sub__(self, other):
        other_lookup = {}
        for row in other.rows.values():
            items = row.get_items()
            other_first_item = items[0]
            if other_first_item not in other_lookup:
                other_lookup[other_first_item] = [items]
            else:
                other_lookup[other_first_item].append(items)
        
        removal_indexes = []
        temp_copy = self.return_copy()
        for idx, self_row in temp_copy.rows.items():
            self_items = self_row.get_items()
            first_item = self_items[0]
            if first_item in other_lookup.keys():
                if self_items in other_lookup[first_item]:
                    removal_indexes.append(idx)
                    
        for index in removal_indexes:
            temp_copy.remove_row_by_index(index, reset_indexes=False)
            
        temp_copy._internal_reset_row_count()    
            
        return temp_copy
        
    def __mod__(self, number):
        row_count = self.return_row_count()
        return row_count % number
        
    ### Len dunder method    
    def __len__(self):
        return self.return_row_count()    
    
    ### Internal Working
    def _internal_increment_column_count(self):
        self.column_count += 1
    def _internal_decrement_column_count(self):
        self.column_count -= 1
    def _internal_increment_row_count(self):
        self.row_count += 1
    def _internal_decrement_row_count(self):
        self.row_count -= 1
    def _internal_decrement_rows_count_by_n(self, n):
        if self.return_row_count() < n:
            raise ValueError(f"Decrement value of {n} is higher than the count of all rows")
        self.row_count -= n
    def _internal_reset_row_count(self):
        self.row_count = len([row for row in self.rows])
    def return_column_count(self):
        return self.column_count       
    def return_row_count(self):
        return self.row_count
    def return_last_row_index(self):
        return self.last_row_index
    def set_last_row_index(self, value):
        self.last_row_index = value
        
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
            column.print()
            #print(f"Index: {column.get_index()}, Name: {column.get_name()}, Data Type: {column.get_data_type()}")

    def print_data_overview(self, filter = "all"):
        print_string = "Data Overview by column"
        for idx, column in self.columns.items():
            name = column.get_name()
            data_type = column.get_data_type()
            
            
            print_string += f"\n\nIndex: {idx}, {name}, Data Type: {data_type}\n\t"      

            if (data_type == "int" or data_type == "decimal") and filter in ["all", "numeric"]:
                min = self.return_min_value(idx)
                max = self.return_max_value(idx)
                range = self.return_range(idx)
                std = self.return_standard_deviation(idx)
                mean = self.return_mean(idx)
                median = self.return_median(idx)
                mode = self.return_mode(idx)
                distincts = len(self.return_distinct_column_values(idx))  
                nulls = self.return_null_count(idx)
                
                print_string += f" - Data Range -> Min: {min}, Max: {max}, Range: {range}\n\t"
                print_string += f" - Averages -> Mean: {mean}, Median: {median}, Mode: {mode[0]} ({mode[1]}), Standard Deviation: {std}\n\t"
                print_string += f" - Data Summary -> Distinct values: {distincts}, NULL values: {nulls}"
                
            elif data_type == "bool" and filter in ["all", "bool"]:
                nulls = self.return_null_count(idx)
                trues = self.return_true_count_from_column(idx)
                falses = self.return_false_count_from_column(idx)
                print_string += f" - Data summary ->  NULL values: {nulls}, True values: {trues}, False values: {falses}"
                
            elif data_type == "string" and filter in ["all", "string"]:
                min = self.return_min_value(idx)
                max = self.return_max_value(idx)
                range = self.return_range(idx)
                mean = self.return_mean(idx)
                median = self.return_median(idx)
                mode = self.return_mode(idx)
                distincts = len(self.return_distinct_column_values(idx))  
                nulls = self.return_null_count(idx)
                
                print_string += f" - Data Range -> Min: {min} ({len(min)}), Max: {max} ({len(max)}), Range: {range}\n\t"
                print_string += f" - Averages -> Mean: {mean}, Median: {median}, Mode: {mode[0]} ({mode[1]})\n\t"
                print_string += f" - Data Summary -> Distinct values: {distincts}, NULL values: {nulls}"
              
            elif data_type == "date" or data_type == "datetime" and filter in ["all", "date"]:
                min = self.return_min_value(idx)
                max = self.return_max_value(idx)
                range = self.return_range(idx)   
                mode = self.return_mode(idx)
                distincts = len(self.return_distinct_column_values(idx))  
                nulls = self.return_null_count(idx)
                
                print_string += f" - Data Range -> Min: {min}, Max: {max}, Range: {range}\n\t"
                print_string += f" - Averages -> Mode: {mode[0]} ({mode[1]})\n\t"
                print_string += f" - Data Summary -> Distinct values: {distincts}, NULL values: {nulls}"
                
        print(print_string)

    def column_names(self) -> str:
        column_names = []
        for column in self.columns.values():
            longest_value = column.get_longest_value()
            white_space = longest_value - len(str(column.get_name()))
            each_side = (white_space // 2)
            padding = each_side * " "
            column_names.append(padding + column.get_name() + padding)
        return " | ".join(column_names)
    
    ### Columns (Internal)
    def _internal_add_column(self, column):
        if type(column) is Column:
            column.set_longest_value(len(column.get_name()))
            self.columns[column.get_index()] = column        
            self._internal_increment_column_count()
        elif type(column) is str:
            new_column = Column(column, self.return_column_count())           
            new_column.set_longest_value(len(column))
            self.columns[new_column.get_index()] = new_column        
            self._internal_increment_column_count()
       
    def _internal_check_datatype_before_conversion(self, index, data_type) -> bool:
        try:            
            for idx, row in self.rows.items():
                items = row.get_items()
                old_value = items[index]
                
                if old_value == "NULL":
                    break
                
                old_data_type = type(old_value)                               
                
                if data_type == "string": items[index] = str(old_value)
                elif data_type == "int": items[index] = int(old_value)
                elif data_type == "decimal": items[index] = float(old_value)
                elif data_type == "bool": items[index] = bool(old_value)
                elif data_type == "date" and old_data_type is datetime.date: items[index] = old_value
                elif data_type == "date" and old_data_type is datetime: items[index] = old_value.date()
                elif data_type == "date" and old_data_type is str: items[index] = datetime.strptime(old_value, "%Y-%m-%d").date()
                elif data_type == "datetime": items[index] = datetime.strptime(old_value, "%Y-%m-%d %H:%M:%S")
                elif data_type == "datetime" and old_data_type is datetime.date: items[index] = datetime.combine(old_value, dtime())
                elif data_type == "datetime" and old_data_type is datetime: items[index] = old_value
                elif data_type == "datetime": items[index] = datetime.strptime(old_value, "%Y-%m-%d %H:%M:%S")
                elif data_type == "NULL": items[index] = "NULL"                      
            return True    
        except:
            print(f"Caught: {old_value} as {old_data_type} at index {idx}")
            return False 
    
    def _internal_set_column_data_as_datatype(self, index, data_type):
        try:            
            for idx, row in self.rows.items():
                items = row.get_items()
                old_value = items[index]
                
                if old_value == "NULL":
                    continue
                
                if data_type == "string": items[index] = str(old_value)
                elif data_type == "int": items[index] = int(old_value)
                elif data_type == "decimal": items[index] = float(old_value)
                elif data_type == "bool": items[index] = string_to_bool(old_value)
                elif data_type == "date" and type(old_value) is datetime.date: items[index] = old_value
                elif data_type == "date" and type(old_value) is datetime: items[index] = old_value.date()
                elif data_type == "datetime" and type(old_value) is datetime: items[index] = old_value
                elif data_type == "datetime" and type(old_value) is datetime.date: items[index] = datetime.combine(old_value, dtime())
                elif data_type == "date" and type(old_value) is str: items[index] = datetime.strptime(old_value, "%Y-%m-%d").date()
                elif data_type == "datetime"  and type(old_value) is str: items[index] = datetime.strptime(old_value, "%Y-%m-%d %H:%M:%S")
                elif data_type == "NULL": items[index] = "NULL"
                
                self.rows[idx].set_items(items)                
            
        except ValueError as ve:
            raise ValueError(f"Value Error setting data type for index {index} to type {data_type} for value {old_value}\n{ve}")
        except TypeError as te:
            raise TypeError(f"Type Error setting data type for index {index} to type {data_type} for value {old_value}\n{te}")
        except Exception as ge:
            print(f"General exeption: {ge}")

    def _internal_insert_data_into_column(self, data:list, column_index = None, column_name = None):
        input_row_length = len(data)
        row_count = self.return_row_count()        
        length_check = input_row_length == row_count       
        
        if length_check == False:
            raise ValueError(f"Input data of count {input_row_length} does not match the row count of the table: {self.row_count}")
                
        if column_index is not None:
            if column_index not in self.columns.keys():
                raise ValueError(f"Input column index {column_index} not in list of known column indexes")                
            
            for row_index, row_object in self.rows.items():
                new_data = data[row_index]
                row_data:list = row_object.get_items()
                row_data.insert(column_index, new_data)   
                self.update_row(row_index, row_data)  
                
        elif column_name is not None:
            for idx, column_object in self.columns.items():
                column_object_name = column_object.get_name()
                if column_name == column_object_name:
                    if idx not in self.columns.keys():
                        raise ValueError(f"Input column name {column_name} not in list of known columns")
                    for row_index, row_object in self.rows.items():
                        new_data = data[row_index]
                        row_data:list = row_object.get_items()
                        row_data.insert(column_index, new_data)
                        self.update_row(row_index, row_data)
                        return
       
    def _internal_validate_return_column_indexes(self, column_indexes = [], column_names = [], raise_error = True) -> list:
        index_list = []
        if column_indexes != []:
            for index in column_indexes:
                if index in self.columns.keys():
                    index_list.append(index)
                else:
                    if raise_error == True: raise KeyError(f"Index {index} not in list of known column indexes")
        elif column_names != []:
            for name in column_names:
                index = self.return_column_index_by_name(name)
                if index is not None:
                    index_list.append(index)
                else:
                    if raise_error == True: raise KeyError(f"Column name {name} not in list of known column names")
        return index_list
     
    def _internal_is_column_iso_8601(self, column_index):
        index = self.check_and_return_column_index(column_index)
        values = self.return_column_data(index)
                
        data_type = self.return_column_object_by_index(index).get_data_type()
        
        if data_type == "NULL":
            return False        
        
        for value in values:
            if value != "NULL":                
                is_8601 = is_iso_8601(value)
                if is_8601 == False:
                    return False
        return True
    
    def _internal_update_column_longest_values(self):        
        for idx, column in self.columns.items():
            column_name_length = len(str(column.get_name()))
            data = self.return_column_data(idx)
            largest_item = column_name_length
            for cell in data:
                length = len(str(cell))
                if length > largest_item:
                    largest_item = length
                    
            column.set_longest_value(largest_item)            
    
    ### Columnns
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
    
    def return_column_index_by_name(self, column_name) -> int:
        for index, column_object in self.columns.items():
            name = column_object.get_name()
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
    
    def return_column_data(self, column_index = None, column_name = None, include_nulls = True) -> list:
        index = self.check_and_return_column_index(column_index=column_index, column_name=column_name)
        output_data = []
        if index is not None:
            
            for idx, row_object in self.rows.items():   
                value = row_object.get_items()[column_index]
                if include_nulls == False and value != "NULL":          
                    output_data.append(value)
                elif include_nulls == True:
                    output_data.append(value) 
        else:
            return None
        return output_data
    
    def return_two_columns_data(self, column_indexes = [], column_names = [], include_nulls = True):
        indexes = self._internal_validate_return_column_indexes(column_indexes=column_indexes, column_names=column_names)
        if len(indexes) == 2:
            index_one = indexes[0]
            index_two = indexes[1]            

            data_list_one = self.return_column_data(index_one)
            data_list_two = self.return_column_data(index_two)
            
            if include_nulls == False:  
                for itteration in len(data_list_one):
                    data_item_one = data_list_two[itteration]
                    data_item_two = data_list_two[itteration]
                    if data_item_one == "NULL" or data_item_two == "NULL":
                        data_list_one.pop(itteration)
                        data_list_two.pop(itteration)
                    
            return data_list_one, data_list_two   
            
        else:
            raise ValueError(f"Must input two valid indexes to return two column data lists")
    
    def analyse_columns(self, column_index = None, column_name = None):
        for column in self.columns.values():
            name = column.get_name()
            index = column.get_index()
            data_type = column.get_data_type()
            allow_analyse = column.get_analyse_property()
            longest_value = column.get_longest_value()
                        
            if column_index is not None and index != column_index:
                continue
            
            if column_name is not None and name != column_name:
                continue
            
            if allow_analyse == False:
                continue
          
            column_data = (row.get_items()[index] for row in self.rows.values())
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
            new_column.set_longest_value(longest_value)
            
            self.columns[index] = new_column   
            
            self._internal_set_column_data_as_datatype(index, data_type)  
                
    def remove_row_data_by_column_index(self, removal_index):
        if removal_index > (self.column_count - 1):
            return None
        for index, row in self.rows.items():
            row_items = row.get_items()
            row_items.pop(removal_index)
            self.rows[index].set_items(row_items)
        
    def remove_column(self, column_index = None, column_name = None) -> Column:
        index = self.check_and_return_column_index(column_index=column_index, column_name=column_name) 
        if index is not None:
            self._internal_decrement_column_count()
            self.remove_row_data_by_column_index(index)
            return self.columns.pop(index)
                 
    def rename_column(self, new_name, column_index = None, column_name = None):
        index = self.check_and_return_column_index(column_index=column_index, column_name=column_name) 
        if index is not None:
            self.columns[index].set_name(new_name)  
                 
    def insert_column(self, name) -> int:
        self.reset_column_indexes()
        new_index = self.return_column_count()
        new_column = Column(name, new_index)
        new_column.set_longest_value(len(new_column.get_name()))
        self.columns[new_index] = new_column
        self._internal_increment_column_count()
        entrees_needed = self.return_row_count()        
        null_data = ["NULL" for iteration in range(entrees_needed)]
        self._internal_insert_data_into_column(null_data, column_index=new_index)   
        return new_index
    
    def insert_column_with_data(self, name:str, data:list, auto_analyse = True):
        new_index = self.insert_column(name)
        self.update_data_in_column(data, column_index=new_index, auto_analyse=auto_analyse)        
                   
    def generate_static_column_data(self, value, column_index = None, column_name = None, auto_analyse = True):
        index = self.check_and_return_column_index(column_index=column_index, column_name=column_name)
        if index is None:
            raise KeyError(f"Index {index} not found in list of known column indexes")
        column_data = [value for iteration in range(self.return_row_count())]
        self.update_data_in_column(column_data, column_index=index)
        if auto_analyse == True:
            self.analyse_columns(column_index=index)               
    
    def replace_string_data_in_column(self, value_to_replace, new_value, column_index = None, column_name = None):
        index = self.check_and_return_column_index(column_index=column_index, column_name=column_name)
        if index is None:
            raise KeyError(f"Index {index} not found in list of known column indexes")
        data_type = self.columns[index].get_data_type()
        if data_type != "string":
            raise ValueError(f"Input column data type must first be a string")
        
        for idx, row in self.rows.items():
            items = row.get_items()            
            old_item = str(items[index])
            new_item = old_item.replace(value_to_replace, new_value)
            items[index] = new_item
            if old_item != new_item and old_item != "NULL":
                self.rows[idx].set_items(items)      
            
    
    def update_data_in_column(self, data:list, column_index = None, column_name = None, auto_analyse = True):
        input_row_length = len(data)
        row_count = self.return_row_count()        
        length_check = input_row_length == row_count       
        
        if length_check == False:
            raise ValueError(f"Input data of count {input_row_length} does not match the row count of the table: {self.row_count}")
        
        column_index = self.check_and_return_column_index(column_index=column_index, column_name=column_name)

        if column_index is None:
            raise ValueError(f"Column index {column_index} is not in list of known column indexes")
             
        for row_index, row_object in self.rows.items():
            new_data = data[row_index]
            row_data:list = row_object.get_items()
            row_data[column_index] = new_data
            self.update_row(row_index, row_data)  

        if auto_analyse == True:
            self.analyse_columns(column_index)         
        
    def set_column_as_static_value(self, value, column_index = None, column_name = None, auto_analyse = True):
        index = self.check_and_return_column_index(column_index=column_index, column_name=column_name)
        if index is not None:
            for idx, row in self.rows.items():
                items = row.get_items()
                items[index] = value
                row.set_items(items)
        if auto_analyse == True:
            self.analyse_columns(index)
        
    def return_table_by_nulls_in_column(self, column_index = None, column_name = None):
        index = self.check_and_return_column_index(column_index = column_index, column_name = column_index)
        if index is not None:
            copy = self.return_copy()
            indexes_to_remove = []
            for idx, row in copy.rows.items():
                items = row.get_items()
                value = items[index]
                if value != "NULL":
                    indexes_to_remove.append(idx)
                    
            for idx in indexes_to_remove:         
                copy.rows.pop(idx)
            copy.reset_row_indexes()
            return copy
      
    def return_true_count_from_column(self, column_index = None, column_name = None) -> int:
        index = self.check_and_return_column_index(column_index=column_index, column_name=column_name)  
        if index is not None:
            column = self.columns[index]
            data_type = column.get_data_type()
            if data_type != "bool":
                raise TypeError(f"Cannot calculate true / false values from a non bool column")
            
            counter = 0
            for row in self.rows.values():
                value = row.get_items()[index]
                if value == True:
                    counter += 1
            return counter                
            
    def return_false_count_from_column(self, column_index = None, column_name = None) -> int:
        index = self.check_and_return_column_index(column_index=column_index, column_name=column_name)  
        if index is not None:
            column = self.return_column_object_by_index(index)            
            data_type = column.get_data_type()
            if data_type != "bool":
                raise TypeError(f"Cannot calculate true / false values from a non bool column")
            
            counter = 0
            for row in self.rows.values():
                value = row.get_items()[index]
                if value == False:
                    counter += 1
            return counter       

    ### Rows (Internal)
    def _internal_add_row(self, row:Row, index = None):       
        if index is not None:
            self.rows[index] = row
        else:
            self.reset_row_indexes()
            new_index = self.return_last_row_index() + 1
            self.rows[new_index] = row      
        self._internal_update_longest_items_by_single_row(row)
    
    def _internal_update_longest_items_by_single_row(self, row:Row):
        itteration = 0
        items = row.get_items()
        for idx, column in self.columns.items():
            column_name = column.get_name()
            if len(column_name) > column.get_longest_value():
                self.columns[idx].set_longest_value(len(column_name))
            
            if len(column_name) > column.get_longest_value():
                self.columns[idx].set_longest_value(len(column_name))
                
            current_longest_value = column.get_longest_value()
            incoming_value = len(str(items[itteration]))    
                     
            if incoming_value > current_longest_value:
                column_object = self.return_column_object_by_index(idx)
                column_object.set_longest_value(incoming_value)
                self.columns[idx] = column_object     
                
            itteration += 1
    
    def _internal_return_row_object_by_index(self, index) -> Row:
        if index in self.rows.keys():
            return self.rows[index]
        else: raise KeyError(f"Row index {index} not in list of known row indexes")
    
    def _internal_return_rows_by_value_recursive(self, input_rows:dict, column_indexes:list, values:list) -> dict:
        if column_indexes == []:
            return input_rows
        
        column_index = column_indexes[0]
        data_type = self.return_column_object_by_index(column_index).get_data_type()
        value_filter = values[0]
        comparrison = "="
        
        if type(value_filter) == list and len(value_filter) == 2:
            comparrison = value_filter[0]
            value_filter = value_filter[1]
        
        filtered_rows = {}
        for idx, row in input_rows.items():
            items = row.get_items()        
            
            item_one = items[column_index]
            item_two = value_filter
            
            non_exact_filter_types = [int, float, datetime, datetime.date]
            
            if comparrison in ["<", ">", "<=", ">="]:
                if item_one == "NULL" or item_two == "NULL":
                    continue
                if data_type not in ["int", "decimal", "date", "datetime"]:
                    raise ValueError(f"Cannot compare non-numeric or datetime-like data without exact comparrisons != and =")     
            
            if comparrison == "=" and items[column_index] == value_filter:
                filtered_rows[idx] = row
            if comparrison == "!=" and items[column_index] != value_filter:
                filtered_rows[idx] = row
            elif comparrison == "<" and items[column_index] < value_filter:
                filtered_rows[idx] = row
            elif comparrison == "<=" and items[column_index] <= value_filter:
                filtered_rows[idx] = row
            elif comparrison == ">" and items[column_index] > value_filter:
                filtered_rows[idx] = row
            elif comparrison == ">=" and items[column_index] >= value_filter:
                filtered_rows[idx] = row
        
        if len(column_indexes) == 1:
            return self._internal_return_rows_by_value_recursive(filtered_rows, [], [])
        else:
            return self._internal_return_rows_by_value_recursive(filtered_rows, column_indexes[1:], values[1:])
    
    ### Rows   
    def add_row(self, row:list):
        if self.return_last_row_index() is None:
            new_index = 0
            self.set_last_row_index(new_index)
        else:
            new_index = self.return_last_row_index() + 1
            self.set_last_row_index(new_index)      
                      
        cleaned_row = replace_missing_with_nulls(row)
        row_object = Row(new_index, cleaned_row)
        self.rows[new_index] = row_object
        self._internal_increment_row_count()        
        self._internal_update_longest_items_by_single_row(row_object)

    def update_row(self, row_index, updated_items):
        row_object = self._internal_return_row_object_by_index(row_index)
        row_object.items = updated_items
        self.rows[row_index] = row_object
        self._internal_update_longest_items_by_single_row(row_object)
    
    def remove_rows_by_column_value(self, value, column_index = None, column_name = None):
        set_index = self.check_and_return_column_index(column_index=column_index, column_name=column_name)
        
        if set_index is None and column_index is not None:
            raise ValueError(f"Column index {column_index} not found in list of known column indexes")
        elif set_index is None and column_name is not None:
            raise ValueError(f"Column name {column_name} not found in list of known column names")
        
        indexes_to_remove = []
        for idx, row in self.rows.items():
            items = row.get_items()
            test_value = items[set_index]
            if test_value == value:
                indexes_to_remove.append(idx)
                
        for index in indexes_to_remove:
            self.rows.pop(index)
            
        self._internal_decrement_rows_count_by_n(len(indexes_to_remove))            
        self.reset_row_indexes()
    
    def remove_row_by_index(self, index:int, reset_indexes = True, reset_longest_value = True):
        if index in self.rows.keys():
            row_object = self.rows[index]
            row_count = self.return_row_count()
            self.rows.pop(index)
            after_count = self.return_row_count()
            if row_count == after_count + 1:
                self._internal_decrement_row_count()
                if reset_indexes == True:
                    self.reset_row_indexes()            
            if reset_longest_value == True:
                self._internal_update_column_longest_values()
        else:
            raise KeyError(f"Row index {index} not found in list of known row indexes")
    
    def return_row_items_by_index(self, index:int) -> list:
        if index in self.rows.keys():
            row_object = self._internal_return_row_object_by_index(index)
            return row_object.get_items()
        else:
            raise ValueError(f"Row index {index} not found in list of known row indexes")

    def return_sub_row_by_index(self, row_index:int, column_indexes = [], column_names = []) -> list:
        if row_index not in self.rows.keys():
            raise ValueError(f"Row index {row_index} not found in list of row indexes")
                
        values = self.return_row_items_by_index(row_index)        
        column_indexes = self._internal_validate_return_column_indexes(column_indexes=column_indexes, column_names=column_names, raise_error=True)
             
        sub_row = []
        for index in column_indexes:
            sub_row.append(values[index])
            
        return sub_row

    def return_distinct_column_values(self, column_index = None, column_name = None, include_nulls = False, sort = True) -> list:
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
                    items = row.get_items()
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

    def return_rows_as_lists(self) -> list[list]:
        return_list = []
        for row in self.rows.values():
            return_list.append(row.get_items())
        return return_list

    def print_row_detailed(self, row_index = None):
        if row_index not in self.rows.keys():
            raise KeyError(f"Index {row_index} not in list of known row indexes")
        data = self.rows[row_index].get_items()
        for idx, column in self.columns.items():
            print(f"Index: {idx}, Name: {column.get_name()}, Data Type: {column.get_data_type()} -> Value: {data[idx]}, Value data type: {type(data[idx])}")

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
   
    def check_and_return_column_index(self, column_index = None, column_name = None) -> int:
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
        
    ### Selection (Internal)              
    def _internal_print_items(self, print_rows) -> str:
        for print_row in print_rows:
            string_list = []
            itteration = 0
            for idx, column in self.columns.items():
                value = print_row[itteration]
                padded = self._internal_modify_string_to_whitespace_padding(value, idx)
                string_list.append(padded)          
                itteration += 1     
            
            print(" | ".join(string_list))
        return print_rows  
    
    def _internal_modify_string_to_whitespace_padding(self, input_value, index) -> str:
        column = self.return_column_object_by_index(index)
        data_type = column.get_data_type()
        longest_value = column.get_longest_value()     
        
        input_string = str(input_value)
        input_length = len(input_string)
        
        if input_length != longest_value:
            whitespace = longest_value - input_length
            padding = whitespace // 2
            
            if data_type in ["string", "bool", "NULL"]:
                return input_string + (whitespace * " ")
            elif data_type in ["int", "decimal"]:
                return (whitespace * " ") + input_string
            elif data_type in ["date", "datetime"]:
                return (padding * " ") + input_string + (padding * " ")
        else:
            return input_string
        
        
    
    def _internal_get_length_dict(self) -> dict:
        spacing_dict = {}
        for idx, column in self.columns.items():
            length = column.get_longest_value()
            spacing_dict[idx] = length
        return spacing_dict
    
    ### Secection
    def top(self, number):
        print(f"\nPrinting top {number} rows")
        columns = self.column_names()        
        print_rows = []
        for idx, row in self.rows.items():
            if len(print_rows) != number:
                print_rows.append(row.get_items())       
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
                print_rows.append(row.get_items())      
                iteration += 1 
        print(columns)  
        print(len(columns) * "-")
        self._internal_print_items(print_rows)  
    
    def random_selection(self, number):
        print(f"\nPrinting {number} random rows")
        columns = self.column_names()                
        table_row_count = self.return_row_count()
        
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
        
        print_items = [row.get_items() for row in self.rows.values()]        
        self._internal_print_items(print_items)
    
    def select(self, column_indexes = [], column_names = [], where_by_index = {}, where_by_name = {}, order_by = [], order_mode = "asc", limit = None, return_subtable = False):
        reference_indexes = self._internal_validate_return_column_indexes(column_indexes = column_indexes, column_names = column_names)
                
        reference_values = []
        check_values = []     

        if where_by_index != {}:
            for column, value in where_by_index.items():                    
                index = self.check_and_return_column_index(column)
                if index != None:
                    reference_values.append(index)
                    check_values.append(value)
        elif where_by_name != {}:
            for column, value in where_by_name.items():                
                index = self.check_and_return_column_index(column)
                if index != None:
                    reference_values.append(index)
                    check_values.append(value)  
                    
        for idx in range(len(check_values)):
            column_index = reference_values[idx]
            check_val = check_values[idx]
            
            data_type = self.return_column_object_by_index(column_index).get_data_type()
            
            logic = "="
            if type(check_val) == list:
                logic = check_val[0]
            
            if data_type not in ["int", "decimal", "date", "datetime"] and logic not in ["!=", "="]:
                raise ValueError(f"Logical comparrison {logic} not allowed for data type {data_type} on column index {column_index}")                    
            
        subtable = self.return_sub_table_by_row_filters(values=check_values, column_indexes=reference_values)
        
        if order_by != []:
            reversed_indexes = reversed(order_by)
            for index in reversed_indexes:
                validated_index = subtable.check_and_return_column_index(index)
                subtable.order_rows_by_column(validated_index, order_mode)
                
        reduced_sub_table = subtable.return_sub_table_by_columns(reference_indexes)        
        
        if limit == None:
            reduced_sub_table.print()
        else:
            reduced_sub_table.top(limit)  
            
        if return_subtable == True:
            return reduced_sub_table
    
    ### Calculations (Internal)    
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
    
    ### Calculations
    def return_max_value(self, column_index = None, column_name = None):
        return self._internal_return_min_max_value("max", column_index = column_index, column_name = column_name)        
        
    def return_min_value(self, column_index = None, column_name = None):
        return self._internal_return_min_max_value("min", column_index = column_index, column_name = column_name)
    
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
            column_type = self.return_column_object_by_index(index).get_data_type()
            
            if dtype is int or dtype is float:
                total = sum(values_only)
                return total / len(values_only)     
            elif dtype is str:
                string_length_list = [len(value) for value in values_only]
                total = sum(string_length_list)
                return total / len(values_only)
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
            column_type = self.return_column_object_by_index(index).get_data_type()
            
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
        
    def return_null_count(self, column_index = None, column_name = None) -> int:
        index = self.check_and_return_column_index(column_index, column_name)
        if index is not None:
            values = self.return_column_data(index)
            nulls = [value for value in values if value == "NULL"]
            return len(nulls)
        
    def return_non_null_count(self, column_index = None, column_name = None) -> int:
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
  
    def return_covariance(self, column_indexes = [], column_names = []) -> float:
        indexes = self._internal_validate_return_column_indexes(column_indexes=column_indexes, column_names=column_names)
        if len(indexes) != 2:
            raise ValueError(f"Covariance calculation requires inputting two columns")
        
        col_one_data_type = self.columns[indexes[0]].data_type
        col_two_data_type = self.columns[indexes[1]].data_type
        
        if col_one_data_type not in ["int", "decimal"] or col_two_data_type not in ["int", "decimal"]:
            raise ValueError(f"Both columns must be either int or decimal data types")
        
        total_n = self.row_count
        column_one_mean = self.return_mean(indexes[0])
        column_two_mean = self.return_mean(indexes[1])
        
        column_one_values = self.return_column_data(indexes[0])
        column_two_values = self.return_column_data(indexes[1])
        
        column_one_calculations = []
        column_two_calculations = []
        for idx in range(total_n):
            val_one = column_one_values[idx]
            val_two = column_two_values[idx]
            
            if val_one != "NULL" and val_two != "NULL":
                column_one_calculations.append(val_one - column_one_mean)
                column_two_calculations.append(val_two - column_two_mean)
        
        product_list = []
        for idx in range(len(column_one_calculations)):
            val_one_minus_mean = column_one_calculations[idx]
            val_two_minus_mean = column_two_calculations[idx]
            product = val_one_minus_mean * val_two_minus_mean
            product_list.append(product)
            
        product_sum = sum(product_list)
        n_minus_one = len(product_list) - 1
        
        if n_minus_one <= 0:
            return None        
        else:
            return product_sum / n_minus_one
    
    def return_correlation_coefficient(self, column_indexes = [], column_names = []):
        indexes = self._internal_validate_return_column_indexes(column_indexes=column_indexes, column_names=column_names)
        covariance = self.return_covariance(column_indexes=column_indexes, column_names=column_names)
        column_one_std = self.return_standard_deviation(indexes[0])
        column_two_std = self.return_standard_deviation(indexes[1])
        
        if column_one_std == 0 or column_two_std == 0:
            return None
        else:        
            return covariance / (column_one_std * column_two_std)
  
    def find_meantingful_correlations(self):
        column_list = []
        results = ""
        for idx, column in self.columns.items():
            data_type = column.get_data_type()
            if data_type == "int" or data_type == "decimal":
                column_list.append(idx)
                
        compared_indexes = []
        for idx in column_list:
            for sub_idx in column_list:
                if idx != sub_idx and [idx, sub_idx] not in compared_indexes:
                    coeffecient = self.return_correlation_coefficient([idx, sub_idx])
                    if coeffecient <= -0.01 or coeffecient >= 0.01:
                        name_one = self.columns[idx].get_name()
                        name_two = self.columns[sub_idx].get_name()
                        results += f"\nColumns: {name_one} and {name_two} coeffecient: {coeffecient}"
            compared_indexes.append([idx, sub_idx])
            compared_indexes.append([sub_idx, idx])
        print("List of all meaningful correlations found in numerical columns")
        if results != "":
            print(results)
        else: print("None...")
  
    ### Calculation Columns
    def add_calculation_column(self, reference_column_index = None, reference_column_name = None, calculation = None, calculation_value = None):
        options = calculation_column_options
        
        if calculation not in options:
            raise KeyError(f"Input option {calculation} not in list of approved options: {options}")
        
        column_object = None
        
        if reference_column_index is not None:
            if reference_column_index not in self.columns.keys():
                raise ValueError(f"Input index {reference_column_index} not in list of known column indexes")
            else:
                column_object = self.columns[reference_column_index]
        
        for idx, column in self.columns.items():
            name = column.get_name()            
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
                
        elif calculation == "days_between":           
            if reference_data_type not in ["date","datetime"]:
                raise ValueError(f"Date calculations require a reference column of date or datetime")
            if type(calculation_value) is not int:
                raise ValueError(f"Input calculation value must be the int")
            second_column_index = self.check_and_return_column_index(calculation_value)
            if second_column_index is None:
                raise ValueError(f"No column of index {calculation_value} found")
            second_column_data_type = self.columns[second_column_index].data_type
            if second_column_data_type not in ["date", "datetime"]:
                raise ValueError(f"Calculation column index {second_column_index} is not a date or datetime")
            
            new_column.set_name("calculated_days_between")
            new_column.set_data_type("int")  
            
            for idx, row in self.rows.items():
                items = row.get_items()
                reference_value = items[reference_index]  
                second_reference_value = items[second_column_index]
                if reference_value != "NULL" and second_reference_value != "NULL":                 
                    days_between_time_delta:timedelta = reference_value - second_reference_value                
                    time_between = days_between_time_delta.days
                    items.append(time_between)
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
            items = row.get_items()
            item_one = items[0]
            
            if items not in row_dict[item_one]:
                row_dict[item_one].append(items)
            else:
                indexes_to_remove.append(idx)               
                
        for index in indexes_to_remove:
            self.rows.pop(index)
            self._internal_decrement_row_count()
        
        self.reset_row_indexes()
        
        return len(indexes_to_remove), indexes_to_remove
        
    def remove_duplicate_rows_by_columns(self, column_indexes = [], column_names = []):        
        sub_table = self.return_sub_table_by_columns(column_indexes = column_indexes, column_names = column_names)
        
        sub_table.reset_column_indexes()
        sub_table.print_structure()

        removals, index_list = sub_table.remove_duplicate_rows()

        for index in index_list:
            self.rows.pop(index)
            self._internal_decrement_row_count()
        
        self.reset_row_indexes()
        
        return removals, index_list   
  
    ### Sub Tables             
    def return_sub_table_by_columns(self, column_indexes = [], column_names = []) -> 'CyberTable':
        found_indexes = self._internal_validate_return_column_indexes(column_indexes=column_indexes,column_names=column_names, raise_error=True )
           
        new_cyber_table = CyberTable()   
        
        for column_index in found_indexes:
            column_object = self.return_column_object_by_index(column_index)
            new_cyber_table._internal_add_column(column_object)
        
        for index in self.rows.keys():           
            sub_row = self.return_sub_row_by_index(index, found_indexes)  
            new_cyber_table.add_row(sub_row)     
            
        new_cyber_table.reset_column_indexes()
        new_cyber_table.reset_row_indexes()    
        new_cyber_table._internal_update_column_longest_values()
        
        return new_cyber_table                 
            
    def return_sub_table_by_row_filters(self, values:list , column_indexes = [], column_names = []) -> 'CyberTable':
        if values == []:
            raise ValueError(f"Number of input items is empty")
        
        found_indexes = self._internal_validate_return_column_indexes(column_indexes=column_indexes,column_names=column_names, raise_error=True )
       
        if len(found_indexes) != len(values):
            raise ValueError(f"Number of found column indexes does not match the number of values inputted")

        filtered_rows = self._internal_return_rows_by_value_recursive(self.rows, found_indexes, values)
        
        new_cyber_table = CyberTable()
        
        for column in self.columns.values():                   
            new_cyber_table._internal_add_column(column)
        
        for idx, row_object in filtered_rows.items():           
            new_cyber_table._internal_add_row(row_object, idx)
            
        new_cyber_table.reset_row_indexes()
        new_cyber_table.reset_column_indexes()
            
        return new_cyber_table        
        
    ### String Functions    
    def clean_string_column(self, column_index = None, column_name = None, capital_first_letter = True):
        found_column_index = self.check_and_return_column_index(column_index = column_index, column_name = column_name)
        column = self.return_column_object_by_index(found_column_index)
        name = column.get_name()
        
        if column.data_type != "string":
            raise TypeError(f"Column {name} is not a string")
        
        for row_idx, row in self.rows.items():
            string_value:str = row.get_items()[found_column_index]
            string_value = string_value.strip()
            
            if capital_first_letter == True:
                first_letter:str = string_value[0]
                first_letter = first_letter.upper()
                string_value = first_letter + string_value[1:]
                
            items = row.get_items()
            items[found_column_index] = string_value
            
            self.rows[row_idx].set_items(items)        
        
    def set_column_string_case(self, case, column_index = None, column_name = None):
        found_column_index = self.check_and_return_column_index(column_index = column_index, column_name = column_name)
        column = self.return_column_object_by_index(found_column_index)
        name = column.get_name()
        
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
            items = row.get_items()
            items[found_column_index] = new_column_data[0]
            new_column_data.pop(0)
            self.rows[idx].set_items(items)       
        
    def convert_iso_8601_string_to_datetime(self, column_index = None, column_name = None):
        index = self.check_and_return_column_index(column_index = column_index, column_name = column_name)    
        column = self.return_column_object_by_index(index)
        
        if column.get_data_type() != "string":
            raise ValueError(f"Input must be a string data type")
        
        update_dict = {}
        
        for idx, row in self.rows.items():
            items = row.get_items()
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
    
    ### File Operations
    def save_as_csv(self, directory, file, delimiter = ","):
        if os.path.isdir(directory) == False:
            raise ValueError(f"Directory {directory} does not exist")
        target_file_path = directory + "\\" + file + ".csv"
        
        if os.path.exists(target_file_path):
            raise ValueError(f"The target file: {target_file_path} already exists")
        
        columns = self.columns.values()
        name_list = [column.get_name() for column in columns]
        
        with open(target_file_path, "a") as writer:        
            writer.write(delimiter.join(name_list) + "\n")            
            for row in self.rows.values():                
                items = [str(item) for item in row.get_items()]
                list_index = 0
                for item in items:
                    if "," in item:
                        items[list_index] = "\"" + item + "\""
                        list_index += 1
                writer.write(delimiter.join(items))
                writer.write("\n")
    
    ### Grouping (Internal)
    def _internal_return_sub_table_groups_recursive(self, tables:list, index_list):
        
        if len(index_list) == 0 or index_list is None:
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
    
    ### Grouping
    def return_groups(self, column_indexes = [], column_names = []) -> 'CyberTableGroup':
        approved_indexes = self._internal_validate_return_column_indexes(column_indexes=column_indexes, column_names=column_names, raise_error=True)               
        
        copy = self.return_copy()
        sub_table_list = self._internal_return_sub_table_groups_recursive([copy], approved_indexes)      
        
        table_group = CyberTableGroup()
        for table in sub_table_list:
            table.row_count = len(table.rows)
            table.reset_row_indexes()
            table_group.add_table(table, approved_indexes)
            
        return table_group
      
    def aggregate(self, command_dict = {}, reference_column_indexes = [], reference_column_names = [], calculation_column_indexes = [], calculation_column_names = [], calculations = []) -> 'CyberTable':
       
        column_index_list = self._internal_validate_return_column_indexes(column_indexes=reference_column_indexes, column_names=reference_column_names, raise_error=True)
        calculation_column_index_list = []
        calculations = calculations
                
        if command_dict != {}:            
            calculations = []
            for identifier, calculation in command_dict.items():
                if identifier is int:                    
                    index = self.check_and_return_column_index(identifier)
                else:
                    index = self.return_column_index_by_name(identifier)
                    
                calculation_column_index_list.append(index)
                calculations.append(calculation)
                
        elif calculation_column_indexes != [] or calculation_column_names != []:
            calculation_column_index_list = self._internal_validate_return_column_indexes(column_indexes=calculation_column_indexes, column_names=calculation_column_indexes, raise_error=True)
               
        if len(calculation_column_index_list) != len(calculations):
            raise KeyError(f"Count of columns {len(column_index_list)} does not match the number of calculations {len(calculations)}")        
        for index in column_index_list:
            if index in calculation_column_index_list:
                raise KeyError(f"Reference columns cannot also be calculation columns, they bust be distinctly separate")        
      
        groups = self.return_groups(column_index_list)
        aggregate_table = groups.aggregate(reference_column_indexes=column_index_list, calculation_column_indexes=calculation_column_index_list, calculations=calculations)
        return aggregate_table       
       
class CyberTableGroup():
    def __init__(self):
        self.table_count = 0
        self.groups = []
        self.columns = {}
        self.grouped_indexes = []
        
    ### Internal
    def _internal_return_column_object_by_index(self, index) -> Column:
        if index in self.columns.keys():
            return self.columns[index]
        else: raise KeyError(f"Column index {index} not in list of known column indexes")
        
    def _internal_check_incoming_table(self, table):
        for idx, column in table.columns.items():
            name = column.get_name()
            data_type = column.get_data_type()
            
            list_index = None
            for column in self.columns.values():
                column_index = column.get_index()
                if column_index == idx:
                    list_index = column_index
                    break
            
            if list_index is None:
                raise KeyError(f"Incoming table column indexes do not match the first table in the group")
            
            existing_column = self._internal_return_column_object_by_index(idx)
            existing_name = existing_column.get_name()
            existing_data_type = existing_column.get_data_type()
            
            if existing_name != name:
                raise KeyError(f"Incoming table column name {name} at index {idx} from new table does not match the column name {existing_name} from the group columns")
            if existing_data_type != data_type:
                raise KeyError(f"Incoming table column {name} data type of {data_type} does not match the existing data type {existing_data_type} from the group column of the same name")
        return True
    
    def _internal_increment_table_count(self):
        self.table_count += 1
    
    def _internal_decrement_table_count(self):
        self.table_count -= 1
    
    def _internal_validate_return_column_indexes(self, column_indexes = [], column_names = [], raise_error = True) -> list:
        index_list = []
        if column_indexes != []:
            for column_object in column_indexes:
                index = column_object.get_index()
                found = False
                for column in self.columns:
                    test_index = column.get_index()
                    if test_index == index:
                        found = True              
                        index_list.append(index)                                        
                if found == False:
                    if raise_error == True: raise KeyError(f"Index {index} not in list of known column indexes")
        elif column_names != []:
            for name in column_names:
                index = self._internal_return_column_index_by_name(name)
                if index is not None:
                    index_list.append(index)
                else:
                    if raise_error == True: raise KeyError(f"Column name {name} not in list of known column names")
        return index_list
    
    def _internal_return_column_index_by_name(self, column_name) -> int:
        for column_object in self.columns:
            name = column_object.get_name()
            if name == column_name:
                return column_object.get_index()   
    
    ### Normal Functions
    def add_table(self, table, group_indexes = []):          
        if self.grouped_indexes == []:
            self.grouped_indexes = group_indexes
            
        if self.table_count == 0:
            for idx, column in table.columns.items():
                name = column.get_name()
                data_type = column.get_data_type()
                permissions = column.get_analyse_property()
                
                new_column = Column(name, idx, data_type)
                new_column.allow_analyse = permissions                
                self.columns[idx] = column
                
            self.groups.append(table)
            self._internal_increment_table_count()
        else:
            result = self._internal_check_incoming_table(table)
            if result == True:
                self.groups.append(table)
                self._internal_increment_table_count()
            else:
                raise ValueError(f"Incoming table does not match the column schema of the group column list")                      
        
    def return_tables(self) -> list:
        return self.groups    
    
    def aggregate(self, command_dict = {}, reference_column_indexes = [], reference_column_names = [], calculation_column_indexes = [], calculation_column_names = [],  calculations = []) -> CyberTable:
        options = aggregation_options
        
        calculation_column_indexes = calculation_column_indexes
        calculations = calculations
        
        for calculation in calculations:
            if calculation not in options:
                raise KeyError(f"Input option {calculation} not in list of approved options: {options}")
        
        reference_column_index_list = self._internal_validate_return_column_indexes(column_indexes=reference_column_indexes, column_names=reference_column_names, raise_error=True)
        
        if command_dict != {}:
            calculations = []
            calculation_column_indexes = []
            for identifier, calculation in command_dict.items():
                if identifier is int:                    
                    index = self.check_and_return_column_index(identifier)
                else:
                    index = self.return_column_index_by_name(identifier)
                    
                calculation_column_indexes.append(index)
                calculations.append(calculation)
                
        elif calculation_column_indexes != [] or calculation_column_names != []:
            calculation_column_indexes = self._internal_validate_return_column_indexes(column_indexes=calculation_column_indexes, column_names=calculation_column_names, raise_error=True)
    
        if len(calculation_column_indexes) != len(calculations):
            raise KeyError(f"Count of columns {len(calculation_column_indexes)} does not match the number of calculations {len(calculations)}")
        
        for index in reference_column_index_list:
            if index in calculation_column_indexes:
                raise KeyError(f"Reference columns cannot also be calculation columns, they bust be distinctly separate")            
        
        aggregate_cybertable = CyberTable()
        
        for idx in calculation_column_indexes:
            name = self._internal_return_column_object_by_index(idx).get_name()
            aggregate_cybertable._internal_add_column(name)
            
        for idx in range(len(calculation_column_indexes)):
            calculation_column_index = calculation_column_indexes[idx]
            calculation = calculations[idx]
            new_name = self._internal_return_column_object_by_index(calculation_column_index).get_name() + "_" + calculation           
            aggregate_cybertable._internal_add_column(new_name)

        row_index = 0
        for table in self.groups:
            aggregate_row = []
            top_row = table.rows[0]
            items = top_row.get_items()
            
            for reference in reference_column_index_list:
                aggregate_row.append(items[reference])
                
            for iteration in range(len(calculation_column_indexes)):
                calculation_idx = calculation_column_indexes[iteration]
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
 
    def merge_into_cyber_table(self) -> CyberTable:
        new_table = CyberTable()     
        
        for column in self.columns:
            new_table._internal_add_column(column)
            
        for table in self.groups:
            for row in table.rows.values():
                new_table.add_row(row.get_items())
                
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
       
# Normal Functions   

### Debugging
def wait(message = None):
    if message == None:
        input("waiting...")
    else:
        input(message)
    
### Strings
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
  
def string_to_bool(input):
    if is_bool(input) == True:
        lower = str(input).lower()
        if lower == "true": return True
        elif lower == "false": return False
       
def convert_iso_8601_to_datetime(input):
    input_string = str(input)
    if "Z" not in input_string or "T" not in input_string:
        raise ValueError(f"Input: {input} doesn't appear to be ISO 8601")
    input_string = input_string.replace("T", " ").replace("Z", "")
    
    if len(input_string) == 23:
            input_string = input_string[:19]

    try:
        new_value = datetime.strptime(input_string, "%Y-%m-%d %H:%M:%S")
        return new_value
    except:
        raise ValueError(f"Input: {input_string} is not a datetime in format yyyy-mm-dd hh:mm:ss")      
          
### Data type checks
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
    
def is_iso_8601(input):
    
    if type(input) is not str:
        return False
    
    length = len(input)
    if length == 24 and input[4] == "-" and input[7] == "-" and input[10] == "T" and input[13] == ":" and input[16] == ":" and input[19] == "." and input[-1] == "Z":
            try:
                datetime_value = convert_iso_8601_to_datetime(input)
                return True
            except:
                return False
    return False

### Nulls    
def replace_missing_with_nulls(input_list:list):
        return_list = []
        for item in input_list:
            if len(str(item)) == 0:
                return_list.append("NULL")
            else: return_list.append(item)
        return return_list
    
### File operations
def open_csv(file, delimiter = ",", null_excess_columns = True) -> CyberTable:
    
    if os.path.exists(file):
        # Create the table object
        cyber_table = CyberTable()
        
        # Open the file and read the lines using the delimiter
        with open(file, "r", encoding="utf-8") as open_file:
            lines = open_file.readlines()           
            column_line = lines[0].rstrip("\n") 
            columns = column_line.split(delimiter)   
            
            row_lines = lines[1:]
            row_lines = [line.encode("cp1252", errors="ignore").decode("cp1252") for line in row_lines]
                    
            # Add column objects to the table
            index = 0
            for column in columns:                
                column_object = Column(column, index)                
                cyber_table._internal_add_column(column_object)
                index += 1
            
            # Add all lines to the table object
            for line in row_lines:
                line_string = line.rstrip("\n")
                line_string = line_string.replace(",,", ",NULL,")
                line_list = line_string.split(delimiter) 
                
                values_corrected = []
                building = False
                builder_string = ""
            
                for idx in range(len(line_list)):
                    value = line_list[idx]                
                
                    if value.startswith("\"") == True and value.endswith("\"") == False:
                        building = True
                        builder_string = value[1:] + ","
                    elif value.startswith("\"") == False and value.endswith("\"") and building == True:
                        builder_string += value[:-1]
                        building = False
                    elif building == True:
                        builder_string += value + ","
                        
                    if building == False and builder_string == "":
                        values_corrected.append(value)
                    elif building == False and builder_string != "":
                        values_corrected.append(builder_string)
                        builder_string = ""      
                        
                # Fill in extra dodgey columns with NULL
                if null_excess_columns == True:
                    columns = len(values_corrected)
                    column_count = cyber_table.return_column_count()
                    difference = column_count - columns
                    if difference > 0:
                        for iteration in range(difference):
                            values_corrected.append("NULL") 
                    
                cyber_table.add_row(values_corrected)               
                              
                
        # Detect data types
        cyber_table.analyse_columns()    
                
        # Return the table object
        return cyber_table
    else:
        return None
    
def round_trip_csv(file, delimiter = ",", convert_iso_8601 = True):
    file_only = str(file).split("\\")[-1][:-4]
    new_file = file_only + "_cleaned"
    
    file_len = len(file_only) + 4
    dir_len = len(file) - file_len
    directory = file[:dir_len - 1]
    
    cyber_table = open_csv(file, delimiter = delimiter)
    
    if convert_iso_8601 == True:
        for key in cyber_table.columns.keys():
            is_8601 = cyber_table._internal_is_column_iso_8601(key)            
            if is_8601 == True:
                cyber_table.convert_iso_8601_string_to_datetime(key)
                
    cyber_table.save_as_csv(directory, new_file, delimiter=delimiter)

### Help
def help():
        documentation = "https://github.com/thecyberdragon/Python-Scripts/blob/main/cyber_tables_documentation.md"
        print(f"====== Cyber Tables ======")
        print(f"Documentaion on GitHub: {documentation}")
        print(f"Options for adding calculation columns: {calculation_column_options}")
        print(f"Options for aggrgating table data: {aggregation_options}")
        print(f"Options for printing data overview: {print_data_overview_options}")


