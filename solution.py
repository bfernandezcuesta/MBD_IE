# 1. (**2 points**) Create a function `read_savings_data` that reads the file named `bank.csv`
# and returns a list of dictionaries, where each element of the list should have
# the form (Poncho & Blanca):

```json
{
  "name": "Leroy",
  "savings": "$7883.30",
  "country": "DE"
}
```

import csv
def read_savings_data (filename):
    data = []
    with open(filename) as file:
        reader = csv.DictReader(file)
        for row in reader:
            data.append(row)
    return data

bank_data = read_savings_data("bank.csv")

# Removing $ from the Values in Key: Savings

def remove_char(my_list):
    for dict in my_list:
        for key, value in dict.items():
            dict['savings'] = dict['savings'].replace('$', '')
        dict['savings'] = float(dict['savings'])
        print(my_list)

remove_char(bank_data)

# Function to write a JSON file from a dictionary (to test if the format is correct):

import json
def write_file(my_dict):
    with open("test_output.json", "w") as output_file:
        json.dump(my_dict, output_file, indent=2)

write_file(bank_data)

# Final List of Dictionaries to use for the rest of the questions:
bank_data

# 2. (**2 points**) Create a function `calculate_total_savings`, that receives a list of
#dictionaries like the one resulting from `read_savings_data`, and computes the total savings
#of all clients in the bank. Do not use the function `sum` provided by Python.

def calculate_total_savings(my_list):
    saving = 0
    for dict in my_list:
        for key, value in dict.items():
            if key == 'savings':
                saving = saving + dict['savings']
    return round(saving, 2)
    
calculate_total_savings(bank_data)
    

#3. (**2 points**) Create a function `richest_person`, that receives a list of dictionares
#like the one resulting from `read_savings_data`, and returns the name of the person that
#has the highest amount of money.


def richest_person(my_list):
    highest = None
    name = '' 
    for dict in my_list:
        for key, value in dict.items():
            if highest is None:
                highest = dict['savings']
            elif dict['savings'] > highest:
                highest = dict['savings']
                name = dict['name']
    return name, highest
            
richest_person(bank_data)        


#4. (**2 points**) Create a function `poorest_person`, that receives a list of dictionaries
#like the one resulting from `read_savings_data`, and returns the name of the person that
#has the lowest amount of money.

def poorest_person(my_list):
    lowest = None
    name = '' 
    for dict in my_list:
        for key, value in dict.items():
            if lowest is None:
                lowest = dict['savings']
            elif dict['savings'] < lowest:
                lowest = dict['savings']
                name = dict['name']
    return name, lowest
            
poorest_person(bank_data)

#5. (**3 points**) Create a function `get_all_countries`, that receives a list of dictionaries
#like the one resulting from `read_savings_data`, and returns a list of all
#different countries, with no duplicates.



def get_all_countries(my_list):
    
    country=[]
    for dict in my_list:
        for key, value in dict.items():
            location =dict['country']
        country.append(location) if location not in country else country
    return country
get_all_countries(bank_data)


#6. (**3 points**) Create a function `calculate_average_savings`, that receives a list of
#dictionaries like the one resulting from `read_savings_data`, and returns the
#average savings of all people in the bank. Do not use the function `sum`
#provided by Python.

def calculate_average_savings(my_list):
    
    savings = 0
    for dict in my_list:
        for key, value in dict.items():
            if key == 'savings':
                savings = savings + dict['savings']
    
    count=0
    for dict in my_list:
        for key, value in dict.items():
            if key == 'savings':
                count =1 + count
    
    return round(savings / count,2)

calculate_average_savings(bank_data)


#7. (**4 points**) Create a function `people_per_country`, that receives a list of dictionaries
#like the one resulting from `read_savings_data`, and returns a dictionary
#where the keys are the different countries in the input, and the values are
#the number of people that are from each country. For example, the output should
#look like:

```json
{
  "ES": 14,
  "DE": 29,
  ...
}
```
def people_per_country(list_dicts):
    new_dict = []
    for dict in list_dicts:
        for key, value in dict.items():
            if key == 'country':
                new_dict.append(dict['country'])
    counter = {x:new_dict.count(x) for x in new_dict}
    return counter

people_per_country(bank_data)

#8. (**2 points**) Create a function `write_report`, that writes a report in a file named 
#`report.json`, with the following format:

```json
{
  "total_savings": "amount",
  "richest_person": "name",
  "poorest_person": "name",
  "all_countries": [
    "ES",
    "IT",
    ...
  ],
  "average_savings": "amount",
  "people_per_country": {
    "ES": 14,
    "DE": 29,
    ...
  }
}
```
import json
def write_report(final_list_report):
    C = ["Tommaso Mercaldo", "Alfonso Villegas", "Blanca Fernandez-Cuesta", "Marcela Zablah", "Stephan Beismann", "Ting Lun Fan"]
    data = {"Group_C" : C,
            "total_savings" : calculate_total_savings(final_list_report),
            "richest_person" : richest_person(final_list_report),
            "poorest_person" : poorest_person(final_list_report),
            "all countries" : get_all_countries(final_list_report),
            "average_savings" : calculate_average_savings(final_list_report),
            "people_per_country": people_per_country(final_list_report)
            }
    with open('report.json', "w") as report:
        json.dump(data, report, indent=2)
    return data

write_report(bank_data)

