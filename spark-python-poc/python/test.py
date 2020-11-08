import configparser as cp
props = cp.RawConfigParser()
props.read("properties.txt")
mode = props.get("dev","executionmode")
print(mode)