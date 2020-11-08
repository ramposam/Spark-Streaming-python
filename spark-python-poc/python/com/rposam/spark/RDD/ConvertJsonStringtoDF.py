import json
from pyspark.sql.types import StructType, StringType, StructField, ArrayType

from com.rposam.spark.config.SparkConf import CreateSparkDriver

schema = StructType([
    StructField("results", ArrayType(
        StructType([
            StructField("user", StructType([
                StructField("gender", StringType()),
                StructField("name", StructType([
                    StructField("title", StringType()),
                    StructField("first", StringType()),
                    StructField("last", StringType())
                ])),
                StructField("location", StructType([
                    StructField("street", StringType()),
                    StructField("city", StringType()),
                    StructField("state", StringType()),
                    StructField("zip", StringType())
                ])),
                StructField("email", StringType()),
                StructField("username", StringType()),
                StructField("password", StringType()),
                StructField("salt", StringType()),
                StructField("md5", StringType()),
                StructField("sha1", StringType()),
                StructField("sha256", StringType()),
                StructField("registered", StringType()),
                StructField("dob", StringType()),
                StructField("phone", StringType()),
                StructField("cell", StringType()),
                StructField("picture", StructType([
                    StructField("large", StringType()),
                    StructField("medium", StringType()),
                    StructField("thumbnail", StringType())
                ]))
            ]))
        ])
    )),
    StructField("nationality", StringType()),
    StructField("seed", StringType()),
    StructField("version", StringType())
])
if __name__ == "__main__":
    jsonStr = '{"results": [{"user": {"gender": "male", "name": {"title": "mr", "first": "jacob", "last": "morris"}, "location": {"street": "2866 andersons bay road", "city": "taupo", "state": "manawatu-wanganui", "zip": 86983}, "email": "jacob.morris@example.com", "username": "yellowpanda311", "password": "normal", "salt": "MEaoNamo", "md5": "3d071120e2f31d4f9987ba5ebe586cdc", "sha1": "2ab4316f493232a24f9603dd36f1eee8a0b9b53c", "sha256": "f61494c71550369ad195918916406783dbafdd1efe00afbe089a9395a30ee141", "registered": 1433191629, "dob": 572602601, "phone": "(990)-539-9640", "cell": "(058)-170-4911", "picture": {"large": "https://randomuser.me/api/portraits/men/43.jpg", "medium": "https://randomuser.me/api/portraits/med/men/43.jpg", "thumbnail": "https://randomuser.me/api/portraits/thumb/men/43.jpg"}}}], "nationality": "NZ", "seed": "fe2fbcab2d6dfa4e0c", "version": "0.8"}'
    spark = CreateSparkDriver()
    sc = spark.sparkContext
    rdd = sc.parallelize([jsonStr])
    print(rdd.takeSample())
    df = spark.read.schema(schema).json(rdd)
    df.show()
