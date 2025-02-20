import mysql.connector
import pandas as pd
import math
from sqlalchemy import create_engine


def setup_database(db, cursor):
    sql_script = """
    CREATE DATABASE IF NOT EXISTS employee_db;
    USE employee_db;

    -- Creating or re-creating staging table which accepts duplicates on Employee_ID.
    DROP TABLE IF EXISTS employee_data_source;
    CREATE TABLE employee_data_source(
        id INT AUTO_INCREMENT PRIMARY KEY,
        Employee_ID INT,
        Name VARCHAR(255),
        Age INT,
        Department VARCHAR(255),
        Date_of_Joining VARCHAR(50),
        Years_of_Experience INT,
        Country VARCHAR(255),
        Salary INT,
        Performance_Rating VARCHAR(50)
    );

    -- Final table with deduplicated data (if needed later)
    DROP TABLE IF EXISTS employee_data;
    CREATE TABLE employee_data (
        Employee_ID INT PRIMARY KEY,
        Name VARCHAR(255),
        Age INT,
        Department VARCHAR(255),
        Date_of_Joining DATE,
        Years_of_Experience INT,
        Country VARCHAR(255),
        Salary INT,
        Performance_Rating VARCHAR(50)
    );
    """
    for statement in sql_script.split(";"):
        if statement.strip():
            cursor.execute(statement)
    db.commit()
    print("Database and tables created successfully!")


def load_csv_into_db(db, cursor):
    try:
        # Read CSV file
        df = pd.read_csv('employee_data_source.csv')
        print("CSV file loaded successfully.")
        print(df.head())

        data = []
        for row in df.values:
            cleaned_row = tuple(
                None if (isinstance(x, float) and math.isnan(x)) else x
                for x in row
            )
            data.append(cleaned_row)

        insert_sql = """
        INSERT INTO employee_data_source 
        (Employee_ID, Name, Age, Department, Date_of_Joining, Years_of_Experience, Country, Salary, Performance_Rating)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(insert_sql, data)
        db.commit()
        print("Raw data inserted into employee_data_source table successfully!")

    except FileNotFoundError:
        print("Error: employee_data_source.csv not found.")
    except Exception as e:
        print(f"An error occurred: {e}")


def load_data_from_db(db):
    engine = create_engine(
        "mysql+mysqlconnector://root:@localhost/employee_db")
    df_db = pd.read_sql("SELECT * FROM employee_data_source;", con=engine)
    return df_db


def display_data(df):
    print("\nData loaded from the database:")
    print(df.head())


def profile_data(df):
    """
    Profiles the raw employee dataset by:
      - Displaying shape and head of the data.
      - Showing missing values per column.
      - Generating a statistical summary for numeric columns.
      - Identifying duplicate Employee IDs.
      - Validating date formats in the 'Date of Joining' column.
      - Checking for inconsistent values in text fields (e.g., Department)
    """
    print("=== Data Profiling Report ===")

    # 1. General information
    print("\nShape of Data:", df.shape)
    print("\nFirst few records:")
    print(df.head(5))

    # 2. Missing Values Analysis
    print("\nMissing Values per Column:")
    print(df.isnull().sum())

    # 3. Statistical Summary for Numeric Columns
    print("\nStatistical Summary for Numeric Columns:")
    print(df.describe())

    # 4. Duplicate Employee IDs and Names
    id_column = "Employee Id" if "Employee Id" in df.columns else "Employee_ID"
    name_column = "Name"
    duplicates = df[df.duplicated(subset=[id_column, name_column], keep=False)]
    if not duplicates.empty:
        print("\nDuplicate Employee ID and Name records found:")
        print(duplicates)
    else:
        print("\nNo duplicate Employee ID and Name records found.")

    # 5. Date Format Validation
    date_column = "Date of Joining" if "Date of Joining" in df.columns else "Date_of_Joining"
    df['Parsed_Date'] = pd.to_datetime(df[date_column], errors='coerce')
    invalid_dates = df[df['Parsed_Date'].isnull() & df[date_column].notnull()]
    if not invalid_dates.empty:
        print("\nRecords with invalid date format in", date_column, ":")
        print(invalid_dates[[id_column, date_column]])
    else:
        print("\nAll", date_column, "values are valid or missing.")
    df.drop(columns=['Parsed_Date'], inplace=True)

    # 6. Check for Inconsistencies in Department Field
    # Before normalization, view raw unique department values.
    print("\nUnique Department values (raw):")
    print(df["Department"].unique())

    return df


def clean_data(df):
    """
    Cleans the raw employee data by:
      - Resolving missing values (leaving them as None if missing).
      - Converting and standardizing the 'Date_of_Joining' column to YYYY-MM-DD.
      - Removing duplicate records based on Employee ID.
      - Normalizing text fields (Name, Department, Country) to a consistent format and stripping extra spaces.
      - Standardizing the Department field by mapping inconsistent names to a standard set.
    """
    df = df.copy()

    # 1. Resolve Missing Values.
    df['Name'] = df['Name'].fillna("Unknown")
    df['Performance_Rating'] = df['Performance_Rating'].fillna("Unknown")
    df['Country'] = df['Country'].fillna("Unknown")
    df['Years_of_Experience'] = df['Years_of_Experience'].fillna(-1)

    # 2. Standardize Date Format.
    df['Date_Parsed'] = pd.to_datetime(df['Date_of_Joining'], errors='coerce')
    df.loc[df['Date_Parsed'].isnull(), 'Date_Parsed'] = pd.to_datetime("1900-01-01")
    df['Date_of_Joining'] = df['Date_Parsed'].dt.strftime("%Y-%m-%d")
    df.drop(columns=['Date_Parsed'], inplace=True)

    # 3. Deduplicate Records.
    id_column = "Employee Id" if "Employee Id" in df.columns else "Employee_ID"
    before_count = df.shape[0]
    df = df.drop_duplicates(subset=[id_column], keep="first")
    after_count = df.shape[0]
    print(
        f"Deduplicated records: dropped {before_count - after_count} duplicate rows.")

    # 4. Normalize Text Fields and strip extra spaces.
    # For Name: Convert to title case after stripping leading/trailing spaces.
    df['Name'] = df['Name'].astype(str).str.strip().str.title()

    # For Department: First remove all whitespace and convert to uppercase.
    df['Department'] = df['Department'].astype(
        str).str.replace(r'\s+', '', regex=True).str.upper()

    # Define a mapping of inconsistent department names (after whitespace removal) to a standard value.
    dept_mapping = {
        "OPERATIONS": "OPERATIONS",
        "OPRATIONS": "OPERATIONS",
        "CUSTOMERSUPPORT": "CUSTOMER SUPPORT",
        "CUSTSUPPORT": "CUSTOMER SUPPORT",
        "HR": "HR",
        "IT": "IT",
        "LOGISTICS": "LOGISTICS",
        "LOGSTICS": "LOGISTICS",
        "LEGAL": "LEGAL",
        "LEGL": "LEGAL",
        "MARKETING": "MARKETING",
        "MARKNG": "MARKETING",
        "MARKTING": "MARKETING",
        "SALES": "SALES",
        "FINANCE": "FINANCE",
        "FIN": "FINANCE",
        "FINANACE": "FINANCE",
        "R&D": "R&D",
        "RND": "R&D",
        "RESEARCH": "R&D",
        "SUPPORT": "CUSTOMER SUPPORT",
        "HUMANRESOURCES": "HR"
    }
    # Map the inconsistent department names to standard names.
    df['Department'] = df['Department'].map(
        dept_mapping).fillna(df['Department'])

    # For Country: Convert to title case after stripping extra spaces.
    if "Country" in df.columns:
        df['Country'] = df['Country'].astype(str).str.strip().str.title()

    return df


def load_clean_data_to_final_table(db, cleaned_df):
    """
    Inserts the cleaned data from the Pandas DataFrame into the final table (employee_data)
    and validates the insertion by querying the table using a SQLAlchemy engine.
    Then, exports the final table data to a CSV file.
    """
    cursor = db.cursor()

    # Create the final table if not exists
    create_final_table_sql = """
    CREATE TABLE IF NOT EXISTS employee_data (
        Employee_ID INT PRIMARY KEY,
        Name VARCHAR(255),
        Age INT,
        Department VARCHAR(255),
        Date_of_Joining DATE,
        Years_of_Experience INT,
        Country VARCHAR(255),
        Salary INT,
        Performance_Rating VARCHAR(50)
    );
    """
    cursor.execute(create_final_table_sql)
    db.commit()

    # Prepare the cleaned data for insertion.
    # Adjust column name "Employee Id" if necessary.
    id_column = "Employee Id" if "Employee Id" in cleaned_df.columns else "Employee_ID"

    insert_sql = """
    INSERT INTO employee_data
    (Employee_ID, Name, Age, Department, Date_of_Joining, Years_of_Experience, Country, Salary, Performance_Rating)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    # Before conversion, replace NaN with None so that they are correctly inserted as SQL NULL.
    cleaned_df = cleaned_df.where(pd.notnull(cleaned_df), None)

    # Convert DataFrame to list of tuples in the correct order,
    # replacing any np.nan with None.
    data_to_insert = [
        tuple(None if pd.isna(x) else x for x in row)
        for row in cleaned_df[[id_column, "Name", "Age", "Department", "Date_of_Joining",
                               "Years_of_Experience", "Country", "Salary", "Performance_Rating"]].values
    ]

    # Debug: Print first 5 tuples.
    print("Sample data to insert:")
    for row in data_to_insert[:5]:
        print(row)

    try:
        cursor.executemany(insert_sql, data_to_insert)
        db.commit()
        print("Clean data inserted into employee_data table successfully!")
    except Exception as e:
        print(f"An error occurred when inserting clean data: {e}")

    # Validate by querying the final table using SQLAlchemy.
    try:
        engine = create_engine(
            "mysql+mysqlconnector://root:@localhost/employee_db")
        final_df = pd.read_sql("SELECT * FROM employee_data;", con=engine)
        print("\nFinal Table Data (employee_data) Preview:")
        print(final_df.head())
        print(f"\nTotal Records in employee_data: {final_df.shape[0]}")
    except Exception as e:
        print(f"An error occurred during final table validation: {e}")

    # Export the cleaned data to a CSV file.
    try:
        final_df.to_csv("employee_db.csv", index=False)
        print("Cleaned data exported to 'employee_db.csv'.")
    except Exception as e:
        print(f"An error occurred while exporting data to CSV: {e}")

    cursor.close()


def main():
    # Stage 1: Connect without specifying the database to create employee_db if it does not exist.
    db = mysql.connector.connect(
        host="localhost",
        user="root",
        password=""
    )
    cursor = db.cursor()
    setup_database(db, cursor)
    load_csv_into_db(db, cursor)
    cursor.close()
    db.close()

    # Stage 2: Reconnect now that employee_db exists.
    db = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="employee_db"
    )

    # Stage 3: Data Quality Issue Identification.
    df_db = load_data_from_db(db)
    display_data(df_db)
    profile_data(df_db)

    # Stage 4: Data Cleaning.
    print("\n=== Data Cleaning Step ===")
    cleaned_df = clean_data(df_db)
    print("Cleaned Data Preview:")
    print(cleaned_df.head())

    # Stage 5: Load Clean Data to Final Table.
    print("\n=== Loading Clean Data to Final Table ===")
    load_clean_data_to_final_table(db, cleaned_df)

    # Stage 6: Export the cleaned data to a CSV file.

    # Close the connection at the end.
    db.close()


if __name__ == "__main__":
    main()
