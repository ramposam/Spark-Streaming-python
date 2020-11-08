import configparser as cp
from pyspark.sql.types import MapType


class JDBCConnection:
    def __init__(self):
        self.config = cp.ConfigParser()
        self.config.read("com/rposam/spark/config/properties.txt")

    def getPostgresConnectoin(self):
        host = self.config.get("postegres_props", "host")
        port = self.config.get("postegres_props", "port")
        user = self.config.get("postegres_props", "user")
        pwd = self.config.get("postegres_props", "password")
        driver = self.config.get("postegres_props", "driver")
        dbname = self.config.get("postegres_props", "dbname")
        props = {
            "url": "jdbc:postgresql://{}:{}/{}".format(host, port, dbname),
            "user": user,
            "password": pwd,
            "driver": driver,
            "batchsize": 25000
        }
        return props
