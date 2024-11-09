class TableDB:
    def __init__(self):
        self.tables = []

    def insert(self, table):
        existing_table = self.get_table(table.table_name)
        if existing_table is None:
            self.tables.append(table)
        else:
            print(f"{table.table_name}: Duplicate table name")

    def get_table(self, table_name):
        for table in self.tables:
            if table.table_name == table_name:
                return table
        return None

class Table:
    def __init__(self, table_name, data):
        self.table_name = table_name
        self.data = data

    def filter(self, condition):
        filtered_data = []
        for row in self.data:
            if condition(row):
                filtered_data.append(row)
        return filtered_data

    def aggregate(self, key, function):
        values = []
        for row in self.data:
            if key in row:
                values.append(float(row[key]))
        if len(values) == 0:
            raise ValueError(f"No values found for key '{key}'")
        result = function(values)
        return result

    def __str__(self):
        return f"Table: {self.table_name}, with {len(self.data)} rows"

import csv, os

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))

cities = []
with open(os.path.join(__location__, 'Cities.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        cities.append(dict(r))

countries = []
with open(os.path.join(__location__, 'Countries.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        countries.append(dict(r))

cities_table = Table("cities", cities)
countries_table = Table("countries", countries)

db = TableDB()
db.insert(cities_table)
db.insert(countries_table)

cities_in_italy = cities_table.filter(lambda x: x['country'] == 'Italy')
cities_in_sweden = cities_table.filter(lambda x: x["country"] == "Sweden")

italy_cities_table = Table("italy_cities", cities_in_italy)
sweden_cities_table = Table("sweden_cities", cities_in_sweden)

db.insert(italy_cities_table)
db.insert(sweden_cities_table)

# Let's write code to
# - print the average temperature for all the cities in Italy
avg_temp_italy = italy_cities_table.aggregate("temperature", lambda x: sum(x)/len(x))
print(f"The average temperature of all the cities in Italy :\n{avg_temp_italy}\n")
# - print the average temperature for all the cities in Sweden
avg_temp_sweden = sweden_cities_table.aggregate("temperature", lambda x: sum(x)/len(x))
print(f"The average temperature of all the cities in Sweden :\n{avg_temp_sweden}\n")
# - print the min temperature for all the cities in Italy
min_temp_italy = italy_cities_table.aggregate("temperature", lambda x: min(x))
print(f"The min temperature of all the cities in Italy :\n{min_temp_italy}\n")
# - print the max temperature for all the cities in Sweden
max_temp_sweden = sweden_cities_table.aggregate("temperature", lambda x: max(x))
print(f"The max temperature of all the cities in Sweden :\n{max_temp_sweden}\n")
max_latitude = cities_table.aggregate("latitude", lambda x: max(x))
print(f"Max latitude for all cities: {max_latitude}\n")
min_latitude = cities_table.aggregate("latitude", lambda x: min(x))
print(f"Min latitude for all cities: {min_latitude}\n")
