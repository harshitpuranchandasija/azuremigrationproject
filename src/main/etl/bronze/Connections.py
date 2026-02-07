from pyspark.sql import SparkSession
class Connections:
    def __init__(self):
        pass

if __name__ == "__main__":
    print(dbutils.secrets.listScopes())
    sql_server_db_name = dbutils.secrets.get(scope='devsqlusername',key='devsqlserverdb')
    sql_server_password = dbutils.secrets.get(scope='devsqlserverpassword',key='devsqlserverpassword')
    


    