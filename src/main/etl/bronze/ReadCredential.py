#import dbutils
class ReadCredentials:
    def __init__(self,env):
        self.env = env
        self._getCredential(self.env)

    def _getCredential(self,env):
        scope_user = env + 'sqlusername'
        scope_password = env + 'sqlserverpassword'
        #Azure Key Vault
        akv_user = env + 'sqlserverdb'
        akv_password = env + 'sqlserverpassword'
        print("*****************")
        print(scope_user)
        print(scope_password)
        print(akv_user)
        print(akv_password)
        print("*****************")

 #       self.sql_server_user = dbutils.secrets.get(scope = '',key = '')
        