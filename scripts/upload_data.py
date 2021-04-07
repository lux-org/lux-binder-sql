import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("postgresql://testuser:testpass@localhost:5432/testdb")

data = pd.read_csv('https://raw.githubusercontent.com/lux-org/lux-datasets/master/data/college.csv')
data.to_sql(name='college', con=engine, if_exists = 'replace', index=False)

data = pd.read_csv('https://raw.githubusercontent.com/lux-org/lux-datasets/master/data/car.csv')
data.to_sql(name='car', con=engine, if_exists = 'replace', index=False)

data = pd.read_csv('https://raw.githubusercontent.com/lux-org/lux-datasets/master/data/employee.csv')
data.to_sql(name='employee', con=engine, if_exists = 'replace', index=False)