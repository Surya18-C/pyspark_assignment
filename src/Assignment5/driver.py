# main.py

from data_processing import create_dataframes, process_data


def main():
    # Create DataFrames
    spark, employee_df, department_df, country_df = create_dataframes()

    # Process the data
    process_data(spark, employee_df, department_df, country_df)


if __name__ == "__main__":
    main()
