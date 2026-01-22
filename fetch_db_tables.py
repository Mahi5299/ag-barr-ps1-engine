import cx_Oracle
from sshtunnel import SSHTunnelForwarder
import os
import pandas as pd

# Set environment variable for Oracle client
os.environ["LD_LIBRARY_PATH"] = "/ftpfiles/oracle/client/instantclient"

print("Before init")
cx_Oracle.init_oracle_client(lib_dir="/ftpfiles/oracle/client/instantclient")
print("After init - Thick mode enabled")

# SSH and DB config
ssh_host = "10.0.1.174"
ssh_port = 22
ssh_user = "oracle"
ssh_private_key_path = "/ftpfiles/Engine/tpptestdb.pem"

oracle_host = "10.0.1.174"
oracle_port = 1521
oracle_service_name = "tstpdb.nprdmersn.nprdmervcn.oraclevcn.com"
oracle_user = "AGBPP"
oracle_password = "Pp55#T30d#C5c0"

# List of tables to fetch
tables = [
    "AG_PP_CHANGEOVER_MATRIX",
    "AG_PP_CIP_DOWNTIME_EVENTS",
    "AG_PP_DOWNTIME_EVENTS",
    "AG_PP_COMPATIBILITY_RULES_SIMPLIFIED",
    "AG_PP_OPTIMIZED_INPUT",
    "AG_PP_OPTIMIZED_OUTPUT",
    "AG_PP_RESOURCE_AVAILABILITY_POST_CALC",
    "AG_PP_RESOURCE_AVAILABILITY",
    "AG_PP_SCHEDULING_CONSTRAINTS",
    "AG_PP_SPECIFIC_RUN_RULES_SIMPLIFIED",
    "AG_PP_SPECIFIC_RUN_RULES"
]


try:
    with SSHTunnelForwarder(
        (ssh_host, ssh_port),
        ssh_username=ssh_user,
        ssh_pkey=ssh_private_key_path,
        remote_bind_address=(oracle_host, oracle_port),
    ) as tunnel:
        print("SSH tunnel established.")

        dsn = cx_Oracle.makedsn("127.0.0.1", tunnel.local_bind_port, service_name=oracle_service_name)

        connection = cx_Oracle.connect(
            user=oracle_user,
            password=oracle_password,
            dsn=dsn
        )
        print("Connected to the Oracle database")

        for table in tables:
            #print(f"Fetching data from table: {table}")
            query = f"SELECT * FROM {table} WHERE SIMULATION_ID='SIM_517'"
            print(query)
            df = pd.read_sql(query, con=connection)
            var_name = f"{table}_df"
            globals()[var_name] = df
            csv_filename = f"data_csv/{table}_SIM_517.csv"
            df.to_csv(csv_filename, index=False)
            print(f"Saved {table} with {len(df)} rows.")

except cx_Oracle.Error as e:
    print(f"Database connection failed: {e}")

