import yaml
class Parameters:
    def __init__(self,file_name):
        self.param_config = self.readParameterFile(file_name)
        self.setParameter(self.param_config)

    def readParameterFile(self,file_name):
        print("Started: Reading from config file")
        with open(file_name,'r') as f:
            config = yaml.safe_load(f)
        print("Completed: Reading from config file")
    
    def setParameter(self,param_config):
        print("Started: Setting the params in Parameter Class")
        self.load_type = param_config["load_type"]
        self.source_table = param_config["source_table"]
        self.target_table = param_config["target_table"]
        self.create_user = param_config["create_user"]
        self.pk_columns = param_config["pk_columns"]
        self.incremental_column = param_config["pk_columns"]
        print("Completed: Setting the params in Parameter Class")

    