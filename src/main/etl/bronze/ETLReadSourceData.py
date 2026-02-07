from argparse import ArgumentParser
class ETLReadSourceData:
    def __init__(self,file_name):
        self.file_name = file_name
        params = Parameters(self.file_name)
        print("Initializing the ETLReadSourceData")

if __name__ == "__main__":
    try:
        print("Reading Arguments")
        parser = ArgumentParser()
        parser.add_argument('--parameterfile', type=str, required=True, help=f"Provide the file for ETLReadSourceData")
        args = parser.parse_args()
        etl_obj = ETLReadSourceData(args.parameterfile)
    except (Exception,ValueError) as e:
        print("ETLReadSourceData Failed")
        print(str(e)[:400])