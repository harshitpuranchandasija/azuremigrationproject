# from argparse import ArgumentParser
# import sys,os

# import sys

# # Add absolute path to src
# #sys.path.append("/Workspace/Repos/azureharshit123@gmail.com/azuremigrationproject/src")

# # Absolute import
# from Parameters import Parameters
# from ReadCredential import ReadCredentials

# # sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "../../../", "")))
# class ETLReadSourceData:
#     def __init__(self,file_name):
#         self.file_name = file_name
#         print("Initializing the ETLReadSourceData")
#         self.params = Parameters(self.file_name)
#         self.credential = ReadCredentials(self.params.env)

#     def run(self):
#         print("Hello")

# if __name__ == "__main__":
#     try:
#         print("Reading Arguments")
#         print(os.getcwd())
#         # print(os.path.abspath(os.path.join(os.getcwd(), "../../../", "")))
#         parser = ArgumentParser()
#         parser.add_argument('--parameterfile', type=str, required=True, help=f"Provide the file for ETLReadSourceData")
#         print("Added Arguments")
#         args = parser.parse_args()
#         etl_obj = ETLReadSourceData(args.parameterfile)
#         etl_obj.run()
#     except (Exception,ValueError) as e:
#         print("ETLReadSourceData Failed")
#         print(str(e)[:400])



from argparse import ArgumentParser
import sys,os

import sys

# Add absolute path to src
#sys.path.append("/Workspace/Repos/azureharshit123@gmail.com/azuremigrationproject/src")

# Absolute import
from Parameters import Parameters
from ReadCredential import ReadCredentials

# sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "../../../", "")))
class ETLReadSourceData:
    def __init__(self,file_name,dbutils):
        self.file_name = file_name
        self.dbutils = dbutils
        print("Initializing the ETLReadSourceData")
        self.params = Parameters(self.file_name)
        self.credential = ReadCredentials(self.params.env)

    def run(self):
        print("Hello")

    def main(parameterfile,dbutils):
        try:
            print("Reading Arguments")
            etl_obj = ETLReadSourceData(parameterfile,dbutils)
            etl_obj.run()
        except (Exception,ValueError) as e:
            print("ETLReadSourceData Failed")
            print(str(e)[:400])












            