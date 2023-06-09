def getSqlData(): 
    driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

    database_host = "<database-host-url>"
    database_port = "1433" # update if you use a non-default port
    database_name = "<database-name>"
    table = "<table-name>"
    user = "<username>"
    password = "<password>"

    url = f"jdbc:sqlserver://{database_host}:{database_port};database={database_name}"

    remote_table = (spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("dbtable", table)
    .option("user", user)
    .option("password", password)
    .load()
    )
    return remote_table