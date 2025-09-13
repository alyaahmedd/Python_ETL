import pandas as pd
import sqlite3
import json
import pydoc
from datetime import datetime
import pyodbc

students_data = 'students.txt'
grades_data = 'api_grades.json'
courses_db = 'courses.db'

students_df = pd.read_csv(students_data, sep='|')
print(students_df)

conn = sqlite3.connect(courses_db)
cursor = conn.cursor()
cursor.execute("select * from courses")
courses_data = cursor.fetchall()
courses_df = pd.DataFrame(courses_data, columns= ['course_id', 'course_name', 'credits'])
cursor.close()
conn.close()

print(courses_df)

with open(grades_data, 'r') as f:
    grades_df = pd.DataFrame(json.load(f))

print(grades_df)


grades_df['Date'] = pd.to_datetime(grades_df['date'])

print(grades_df['Date'] )

dim_date = grades_df[['Date']].drop_duplicates().reset_index(drop = True)
print(dim_date)

dim_date['DateKey'] = dim_date['Date'].dt.strftime('%Y%m%d').astype(int)
dim_date[['Year','Month','Day']] = dim_date['Date'].dt.strftime('%Y-%m-%d').str.split('-', expand=True).astype(int)
print(dim_date)

fact_df = grades_df.merge(students_df[['student_id']], on='student_id', how='left')
fact_df = fact_df.merge(courses_df[['course_id']], on='course_id', how='left')
fact_df = fact_df.merge(dim_date[['Date', 'DateKey']], on='Date', how='left')
fact_df = fact_df[['student_id', 'course_id', 'grade', 'attendance', 'DateKey']]
fact_df = fact_df.astype({
    'student_id': int,
    'course_id': int,
    'grade': int,
    'attendance': int,
    'DateKey': int
})

server = 'localhost'
database = 'StudentPerformanceDW'
driver = '{ODBC Driver 17 for SQL Server}'

conn = pyodbc.connect(
    f'DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=yes;'
)

students_data = students_df[['student_id', 'name', 'gender', 'city']].astype(str).values.tolist()

courses_data = courses_df[['course_id', 'course_name', 'credits']].astype(object).values.tolist()
dates_data = dim_date[['DateKey', 'Date', 'Year', 'Month', 'Day']].copy()
dates_data['Date'] = dates_data['Date'].dt.date
dates_data = dates_data.values.tolist()
fact_data = fact_df.values.tolist()


cursor = conn.cursor()


cursor.executemany(
    "INSERT INTO dim_student (StudentID, Name, Gender, City) VALUES (?, ?, ?, ?)",
    students_data
)

conn.commit()


cursor.executemany(
    "INSERT INTO dim_course (CourseID, CourseName, Credits) VALUES (?, ?, ?)",
    courses_data
)


cursor.executemany(
    "INSERT INTO dim_date (DateKey, FullDate, Year, Month, Day) VALUES (?, ?, ?, ?, ?)",
    dates_data
)


cursor.executemany(
    """INSERT INTO fact_student_performance (StudentID, CourseID, Grade, Attendance, DateKey)
       VALUES (?, ?, ?, ?, ?)""",
    fact_data
)

conn.commit()
cursor.close()
conn.close()



