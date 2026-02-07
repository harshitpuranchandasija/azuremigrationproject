
class ReadCredentials:
    def __init__(self,env,dbutils):
        self.env = env
        self.dbutils = dbutils
        self._getCredential(self.env,self.dbutils)

    def _getCredential(self,env,dbutils):
        scope_user = env + 'sqlusername'
        scope_password = env + 'sqlserverpassword'
        #Azure Key Vault
        akv_user = env + 'sqlserverdb'
        akv_password = env + 'sqlserverpassword'
        print("BEGIN: *******SETTING CRED**********")
        self.sql_server_user = dbutils.secrets.get(scope = scope_user,key = akv_user)
        self.sql_server_pass = dbutils.secrets.get(scope = scope_password,key = akv_password)
        print("COMPLETED: *******SETTING CRED**********")
        