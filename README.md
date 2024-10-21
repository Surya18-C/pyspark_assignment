# pyspark_assignment

Question:
1.Create DataFrame as purchase_data_df, product_data_df with custom schema with the below data

2.Find the customers who have bought only iphone13

3.Find customers who upgraded from product iphone13 to product iphone14

4.Find customers who have bought all models in the new Product Data



2.Question:

DataSet:Column(“card_number”) ("1234567891234567",),  ("5678912345671234",),  ("9123456712345678",),  ("1234567812341122",),  ("1234567812341342",)

1.Create a Dataframe as credit_card_df with different read methods

print number of partitions

Increase the partition size to 5

Decrease the partition size back to its original partition size

5.Create a UDF to print only the last 4 digits marking the remaining digits as * Eg: ************4567

6.output should have 2 columns as card_number, masked_card_number(with output of question 2)


3.Question:
Create a Data Frame with custom schema creation by using Struct Type and Struct Field 2.Column names should be log_id, user_id, user_activity, time_stamp using dynamic function

Write a query to calculate the number of actions performed by each user in the last 7 days

Convert the time stamp column to the login_date column with YYYY-MM-DD format with date type as its data type

Write the data frame as a CSV file with different write options except (merge condition)

Write it as a managed table with the Database name as user and table name as login_details with overwrite mode.



4. Question
Read JSON file provided in the attachment using the dynamic function

flatten the data frame which is a custom schema

find out the record count when flattened and when it's not flattened(find out the difference why you are getting more count)

Differentiate the difference using explode, explode outer, posexplode functions

Filter the id which is equal to 0001

convert the column names from camel case to snake case

Add a new column named load_date with the current date

create 3 new columns as year, month, and day from the load_date column

write data frame to a table with the Database name as employee and table name as employee_details with overwrite mode, format as JSON and partition based on (year, month, day) using replacing where condition on year, month, day



5.Question
create all 3 data frames as employee_df, department_df, country_df with custom schema defined in dynamic way

Find avg salary of each department

Find the employee’s name and department name whose name starts with ‘m’

Create another new column in employee_df as a bonus by multiplying employee salary *2

Reorder the column names of employee_df columns as (employee_id,employee_name,salary,State,Age,department)

Give the result of an inner join, left join, and right join when joining employee_df with department_df in a dynamic way

Derive a new data frame with country_name instead of State in employee_df Eg(11,“james”,”D101”,”newyork”,8900,32)

convert all the column names into lowercase from the result of question 7in a dynamic way, add the load_date column with the current date

create 2 external tables with parquet, CSV format with the same name database name, and 2 different table names as CSV and parquet format.
