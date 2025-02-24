# Employee Data ELT Pipeline

This repository contains the complete ELT pipeline for extracting data from a CSV file, cleaning and transforming it, and finally loading the data into a MySQL database. The pipeline focuses on data quality by profiling, cleaning, and standardizing data before analysis.

[![IMAGE ALT TEXT HERE](https://img.youtube.com/vi/CAH5PACT4vI/0.jpg)](https://www.youtube.com/watch?v=CAH5PACT4vI)

## Setup Instructions

1. **Clone the Repository**

   ```bash
   git clone https://github.com/YieumYoon/CSDS397_ELT.git
   cd CSDS397_ELT
   ```

2. **Install Dependencies**

   Run the following command to install required Python libraries:

   ```bash
   pip install -r requirements.txt
   ```

3. **Database Setup**

   - Make sure MySQL is installed and running on your machine.
   - Check or update the database credentials in `csds397_ia2_create_database.py` if necessary. By default, the connection is set as:
  
     ```python
     db = mysql.connector.connect(
         host="localhost",
         user="root",
         password="",
         database="employee_db"
     )
     ```

   - The script will automatically create the database (`employee_db`) and the necessary tables.

## How to Run the Pipeline

Execute the primary script with:

```bash
python csds397_ia2_create_database.py
```
