import pandas as pd
import psycopg2
import csv

from sqlalchemy import create_engine

data = pd.read_csv("https://raw.githubusercontent.com/lux-org/lux-datasets/master/data/employee.csv")
engine = create_engine("postgresql://postgres:lux@localhost:5432")
data.to_sql(name="employee", con=engine, if_exists="replace", index=False)

conn = psycopg2.connect("host=localhost dbname=postgres user=postgres password=lux")
cur = conn.cursor()
cur.execute(
    """
    DROP TABLE IF EXISTS cars
    """
)
# create car table in postgres database
cur.execute(
    """
    CREATE TABLE employee(
    Age integer,
    Attrition text,
    BusinessTravel text,
    DailyRate integer,
    Department text,
    DistanceFromHome integer, 
    Education integer,
    EducationField text,
    EmployeeCount integer,
    EmployeeNumber integer,
    EnvironmentSatisfaction integer,
    Gender text,
    HourlyRate integer,
    JobInvolvement integer,
    JobLevel integer,
    JobRole text,
    JobSatisfaction integer,
    MaritalStatus text,
    MonthlyIncome integer,
    MonthlyRate integer,
    NumCompaniesWorked integer,
    Over18 text,
    OverTime text,
    PercentSalaryHike integer,
    PerformanceRating integer,
    RelationshipSatisfaction integer,
    StandardHours integer,
    StockOptionLevel integer,
    TotalWorkingYears integer,
    TrainingTimesLastYear integer,
    WorkLifeBalance integer,
    YearsAtCompany integer,
    YearsInCurrentRole integer,
    YearsSinceLastPromotion integer,
    YearsWithCurrManager integer
)
"""
)

# open employee.csv and read data into database
import urllib.request

target_url = "https://raw.githubusercontent.com/lux-org/lux-datasets/master/data/employee.csv"
for line in urllib.request.urlopen(target_url):
    decoded = line.decode("utf-8")
    if "Age" not in decoded:
        cur.execute(
            "INSERT INTO employee VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", decoded.split(",")
        )
conn.commit()

