import os
import zipfile
import shutil
import sqlite3
import pandas as pd


class Pipeline:

    def __init__(self):
        self.customers = None
        self.transactions = None

    def extract(self):
        """
        Data source: Technical_Data.zip
        Description:
            customers data for different dates
            transactions data for different dates
        """
        # unzipping the csv files and storing in data folder
        zip_file = zipfile.ZipFile('./Technical_Data.zip', 'r')
        zip_file.extractall("./data")
        zip_file.close()

        files = os.listdir("./data/Test_Data")

        for file in files:
            zip_file = zipfile.ZipFile(f'./data/Test_Data/{file}', 'r')
            zip_file.extractall('./data')
            zip_file.close()
        shutil.rmtree("./data/Test_Data")

        # read csv to dataframe and combine dataframes
        files = os.listdir("./data")
        customer_frames = []
        transaction_frames = []

        for file in files:
            file_df = pd.read_csv(f"./data/{file}")
            if "customers" in file:
                customer_frames.append(file_df)
            elif "transactions" in file:
                transaction_frames.append(file_df)

        self.customers = pd.concat(customer_frames)
        self.transactions = pd.concat(transaction_frames)

    def transform(self):
        # formatting customers dataset
        self.customers.rename(
            columns={"First Name": "First_Name",
                     "Last Name": "Last_Name",
                     "State_Abbr": "State",
                     "Zip": "Zip_Code"},
            inplace=True
        )
        self.customers.columns = self.customers.columns.str.lower()

        # formatting transactions dataset
        self.transactions = self.transactions.drop(
            columns=["Unnamed: 0", "Unnamed: 0.1"])

        # NaN Discounts 627618 out of 655605
        # discuss relevance with Data Science/Analysts
        self.transactions.columns = self.transactions.columns.str.lower()

    def load(self):
        db = DataBase()
        self.customers.to_sql("customers", db.conn,
                              if_exists="append", index=False)
        self.transactions.to_sql(
            "transactions", db.conn, if_exists="append", index=False)


class DataBase:

    def __init__(self, db_file="db.sqlite"):
        self.conn = sqlite3.connect(db_file)
        self.cur = self.conn.cursor()
        self.__init_db()

    def __del__(self):
        self.conn.commit()
        self.conn.close()

    def __init_db(self):

        customers_table = f"""CREATE TABLE IF NOT EXISTS customers(
              customer_id VARCHAR(50) NOT NULL,
              first_name VARCHAR(50),
              last_name VARCHAR(50) NOT NULL,
              address VARCHAR(100),
              city VARCHAR(25),
              state VARCHAR(5),
              zip_code FLOAT,
              start_date VARCHAR(20) NOT NULL
                );"""

        transactions_table = f"""CREATE TABLE IF NOT EXISTS transactions(
              date VARCHAR(20) NOT NULL,
              transaction_id INTEGER NOT NULL,
              customer_id VARCHAR(50) NOT NULL,
              department VARCHAR(20),
              category VARCHAR(20),
              sku VARCHAR(20),
              price FLOAT,
              discount FLOAT
                );"""

        self.cur.execute(customers_table)
        self.cur.execute(transactions_table)


if __name__ == "__main__":
    pipeline = Pipeline()
    print('\nData Pipeline created\n')
    print('\t extracting data from source .... ')
    pipeline.extract()
    print('\t formatting and transforming data ... ')
    pipeline.transform()
    print('\t loading into database ... ')
    pipeline.load()

    print('\nDone. See: result in "db.sqlite"')
