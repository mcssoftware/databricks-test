def getSqlData(spark,tablename): 
    driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

    database_host = "myplus-dbx.database.windows.net"
    database_port = "1433" # update if you use a non-default port
    database_name = "myplus"
    user = "sbhandari"
    password = "vDUwaSAGnZGb5rqHIrrhdQaoLPL7fyxR"

    remote_table = (spark.read
        .format("sqlserver")
        .option("host", database_host)
        .option("port", database_port) # optional, can use default port 1433 if omitted
        .option("user", user)
        .option("password", password)
        .option("database", database_name)
        .option("dbtable", tablename) # (if schemaName not provided, default to "dbo")
        .load()
        )
    return remote_table