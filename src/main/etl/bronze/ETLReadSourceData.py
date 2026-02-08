from argparse import ArgumentParser
import sys,os
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
# Add absolute path to src
#sys.path.append("/Workspace/Repos/azureharshit123@gmail.com/azuremigrationproject/src")

# Absolute import
from src.main.etl.bronze.Parameters import Parameters
from src.main.etl.bronze.ReadCredential import ReadCredentials
from src.main.etl.bronze.Connections import Connections

# sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "../../../", "")))
class ETLReadSourceData:
    def __init__(self,file_name,dbutils):
        self.file_name = file_name
        self.dbutils = dbutils
        print("Initializing the ETLReadSourceData")
        self.params = Parameters(self.file_name)
        self.app_name = "ETLReadSourceData" + '_' + self.params.source_table
        self.spark = SparkSession.builder.appName(self.app_name).getOrCreate()
        self.credential = ReadCredentials(self.params.env,dbutils)
        self.conn = Connections(self.credential,self.params,self.spark,self.dbutils)

    def transform(self,src_df):
        print("BEGIN: Tranforming the dataframe")
        transformed_df =  src_df.withColumn("create_user",lit(self.params.create_user))
        transformed_df.show(10)
        print("COMPLETED: Tranforming the dataframe")
        return transformed_df

    def run(self):
        try:
            run_status="Failed"
            src_df = self.conn.get_source_df()
            transformed_df = self.transform(src_df)
            if len(transformed_df.take(1)) > 0:
                self.conn.write_to_adls_delta(transformed_df,self.params,self.dbutils)
            run_status = 'Success'
            error=None
        except (Exception,ValueError) as e:
            print("ETLReadSourceData Failed")
            error = str(e)[:250]
            print(e)
        finally:
            self.conn.write_to_audit_delta(self.app_name,self.params.source_table,self.params.batch_load,run_status,error)

def main(parameterfile,dbutils):
    try:
        print("Reading Arguments")
        etl_obj = ETLReadSourceData(parameterfile,dbutils)
        etl_obj.run()
    except (Exception,ValueError) as e:
        print("ETLReadSourceData Failed")
        print(e)














            