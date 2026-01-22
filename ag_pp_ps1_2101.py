"""
AG Barr Production Planning Engine - Integrated Version
========================================================
This script integrates database connectivity with production planning algorithms.
It fetches data from Oracle database, processes it through various planning stages,
and generates optimized production schedules.

Author: Integration Script
Date: 2025
"""

import cx_Oracle
import itertools
from sshtunnel import SSHTunnelForwarder
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import re
import logging
import os
import sys
from fastapi import FastAPI, Request, BackgroundTasks, HTTPException
from fastapi.responses import JSONResponse

# FastAPI app instance
app = FastAPI()

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ag_barr_production_planning.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# DATABASE CONNECTION FUNCTIONS
# ============================================================================

def get_oracle_connection():
    """
    Establishes and returns an Oracle database connection through an SSH tunnel.
    
    Returns:
        tuple: (connection, tunnel) where connection is the Oracle connection 
               and tunnel is the SSH tunnel (needs to be closed later)
    """
    os.environ["LD_LIBRARY_PATH"] = "/ftpfiles/oracle/client/instantclient"
    
    logger.info("Attempting to establish Oracle connection through SSH tunnel")
    
    try:
        cx_Oracle.init_oracle_client(lib_dir="/ftpfiles/oracle/client/instantclient")
        logger.info("After init - Thick mode enabled")
    except Exception as e:
        logger.info(f"Oracle client already initialized: {e}")
    
    # SSH and Oracle connection parameters
    ssh_host = "10.0.1.174"
    ssh_port = 22
    ssh_user = "oracle"
    ssh_private_key_path = "/ftpfiles/Engine/tpptestdb.pem"
    
    oracle_host = "10.0.1.174"
    oracle_port = 1521
    oracle_service_name = "tstpdb.nprdmersn.nprdmervcn.oraclevcn.com"
    oracle_user = "AGBPP"
    oracle_password = "Pp55#T30d#C5c0"
    
    tunnel = SSHTunnelForwarder(
        (ssh_host, ssh_port),
        ssh_username=ssh_user,
        ssh_pkey=ssh_private_key_path,
        remote_bind_address=(oracle_host, oracle_port),
        local_bind_address=("127.0.0.1",)
    )
    tunnel.start()
    logger.info(f"SSH tunnel established on local port {tunnel.local_bind_port}")
    
    dsn = cx_Oracle.makedsn("127.0.0.1", tunnel.local_bind_port, service_name=oracle_service_name)
    conn = cx_Oracle.connect(
        user=oracle_user,
        password=oracle_password,
        dsn=dsn
    )
    
    logger.info("Oracle connection established successfully")
    return conn, tunnel


def fetch_simulation_data(conn, simulation_id):
    """
    Fetches all required data for a simulation from Oracle database.
    
    Parameters:
        conn: Oracle database connection
        simulation_id: Simulation ID to fetch data for
        
    Returns:
        dict: Dictionary containing all dataframes needed for processing
    """
    logger.info(f"Fetching data for simulation: {simulation_id}")
    
    try:
        dataframes = {
            "df_resource_availability": pd.read_sql(
                f"""SELECT * FROM ag_pp_resource_availability 
                    WHERE simulation_id = '{simulation_id}'""",
                con=conn
            ),
            "df_downtime": pd.read_sql(
                f"""SELECT * FROM ag_pp_downtime_events 
                    WHERE simulation_id = '{simulation_id}'""",
                con=conn
            ),
            "df_CIP_DOWNTIME": pd.read_sql(
                f"""SELECT * FROM ag_pp_cip_downtime_events 
                    WHERE simulation_id = '{simulation_id}'""",
                con=conn
            ),
            "df_run_rules": pd.read_sql(
                f"""SELECT * FROM ag_pp_specific_run_rules_simplified 
                    WHERE simulation_id = '{simulation_id}'""",
                con=conn
            ),
            "df_input": pd.read_sql(
                f"""SELECT * FROM ag_pp_optimized_input 
                    WHERE simulation_id = '{simulation_id}'""",
                con=conn
            ),
            "df_change_over_matrix": pd.read_sql(
                f"""SELECT * FROM ag_pp_changeover_matrix 
                    WHERE simulation_id = '{simulation_id}'""",
                con=conn
            )
        }
        
        logger.info(f"Successfully fetched data for simulation {simulation_id}")
        logger.info(f"  - Resource Availability: {len(dataframes['df_resource_availability'])} rows")
        logger.info(f"  - Downtime Events: {len(dataframes['df_downtime'])} rows")
        logger.info(f"  - CIP Downtime: {len(dataframes['df_CIP_DOWNTIME'])} rows")
        logger.info(f"  - Run Rules: {len(dataframes['df_run_rules'])} rows")
        logger.info(f"  - Input Orders: {len(dataframes['df_input'])} rows")
        logger.info(f"  - Changeover Matrix: {len(dataframes['df_change_over_matrix'])} rows")
        
        return dataframes
        
    except Exception as e:
        logger.error(f"Error fetching simulation data: {e}")
        raise


def is_connection_alive(conn):
    """Check if Oracle connection is still alive."""
    try:
        conn.ping()
        return True
    except Exception as e:
        logger.warning(f"Oracle connection ping failed: {e}")
        return False


def insert_sync_status(conn, simulation_id, status, error_msg=None):
    """
    Safely inserts sync status. Reconnects Oracle + SSH tunnel if needed.
    Returns: tuple (success_flag: bool, conn, tunnel)
    """
    local_conn = conn
    local_tunnel = None
    used_temp_connection = False

    try:
        if conn is None or not is_connection_alive(conn):
            logger.warning("Oracle connection is None or inactive. Attempting reconnect.")
            local_conn, local_tunnel = get_oracle_connection()
            used_temp_connection = True

        cursor = local_conn.cursor()
        cursor.execute("TRUNCATE TABLE AG_PP_SYNC_PYTHON_COUNT")
        cursor.execute("""
            INSERT INTO AG_PP_SYNC_PYTHON_COUNT 
                (SIMULATION_ID, TIME_STAMP, STATUS, ERROR_MSG)
            VALUES (:1, :2, :3, :4)
        """, (simulation_id, datetime.now(), status, error_msg))
        local_conn.commit()
        cursor.close()

        logger.info("✅ Sync status updated successfully.")
        return True, local_conn, local_tunnel
    except Exception as e:
        logger.exception(f"❌ Failed to insert sync status: {e}")
        return False, conn, None
    finally:
        if used_temp_connection:
            try:
                if local_conn:
                    local_conn.close()
                    logger.info("Closed temporary Oracle connection.")
                if local_tunnel:
                    local_tunnel.stop()
                    logger.info("Closed temporary SSH tunnel.")
            except Exception as cleanup_err:
                logger.warning(f"⚠️ Cleanup error: {cleanup_err}")


def generate_oracle_insert_statements(json_data, table_name):
    """
    Converts a list of dictionaries to Oracle-compatible INSERT statements with uniform datetime/timestamp format.
    """
    oracle_datetime_format = "DD-MM-YYYY HH24:MI:SS"
    oracle_timestamp_format = "YYYY-MM-DD HH24:MI:SS.FF6"

    number_fields = {
        "ROW_ID", "PLAN_ID", "ORDER_LINE_NUMBER", "ITEM_ID",
        "FIXED_LOT_MULTIPLIER", "MINIMUM_ORDER_QUANTITY", "MAXIMUM_ORDER_QUANTITY",
        "CONVERSION_FACTOR", "QUANTITY", "QUNATITY_CASE", "QUNATITY_PALLET", "QUNATITY_LITRE",
        "WO_REFERENCE_NUMBER", "RUN_SPEED", "TOTAL_HOURS_FOR_QTY", "TOTAL_DAYS_FOR_QTY",
        "BC_SEQUENCE", "BC_QUANTITY_PRODUCED", "BC_QUANTITY_PRODUCED_CASE",
        "BC_QUANTITY_PRODUCED_PALLET", "BC_QUANTITY_PRODUCED_LITRE",
        "FINAL_TOTAL_HOURS_FOR_QTY", "FINAL_TOTAL_DAYS_FOR_QTY",
        "PALLETS_QUANTITY", "LITRE_QUANTITY", "PALLETS_QUANTITY_PRODUCED", "LITRE_QUANTITY_PRODUCED"
    }

    # Enhanced date parsing to handle TIMESTAMP with fractional seconds
    def format_date_string(value: str):
        try_formats = [
            ("%Y-%m-%d %H:%M:%S.%f", "TO_TIMESTAMP('{}', 'YYYY-MM-DD HH24:MI:SS.FF6')"),
            ("%Y-%m-%d %H:%M:%S", "TO_DATE('{}', 'YYYY-MM-DD HH24:MI:SS')"),
            ("%d-%m-%Y %H:%M:%S", "TO_DATE('{}', 'DD-MM-YYYY HH24:MI:SS')"),
            ("%d-%m-%Y %H:%M", "TO_DATE('{}', 'DD-MM-YYYY HH24:MI')"),
            ("%d-%m-%Y", "TO_DATE('{}', 'DD-MM-YYYY')"),
            ("%d%m%Y %H:%M:%S", "TO_DATE('{}', 'DDMMYYYY HH24:MI:SS')")
        ]
        for fmt, oracle_fmt in try_formats:
            try:
                dt = datetime.strptime(value, fmt)
                return oracle_fmt.format(dt.strftime(fmt))
            except ValueError:
                continue
        return None

    def format_value(key, value):
        if value is None:
            return "NULL"
        if isinstance(value, str):
            formatted_date = format_date_string(value)
            if formatted_date:
                return formatted_date
        if key in number_fields:
            try:
                return str(float(value)) if '.' in str(value) else str(int(value))
            except (ValueError, TypeError):
                return "NULL"
        if isinstance(value, str):
            escaped = value.replace("'", "''")
            return f"'{escaped}'"
        return str(value)

    insert_statements = []
    for row in json_data:
        columns = list(row.keys())
        values = [format_value(k, row[k]) for k in columns]
        col_str = ", ".join(columns)
        val_str = ", ".join(values)
        insert_stmt = f"INSERT INTO {table_name} ({col_str}) VALUES ({val_str})"
        insert_statements.append(insert_stmt)

    return insert_statements


# Function to convert DataFrame row to dictionary, handling nulls and data types
def row_to_dict(row):
    row_dict = row.to_dict()
    for key, value in row_dict.items():
        if pd.isna(value):  # Convert NaN/null to None for Oracle
            row_dict[key] = None
        elif isinstance(value, pd.Timestamp):  # Convert pandas Timestamp to string
            row_dict[key] = value.strftime("%d-%m-%Y %H:%M:%S")
        elif isinstance(value, float) and value.is_integer():  # Convert float integers to int
            row_dict[key] = int(value)
    return row_dict






# Function to generate the INSERT SQL statement dynamically
def generate_insert_sql(table, columns):
    columns_str = ", ".join(columns)
    placeholders = ", ".join([":" + str(i + 1) for i in range(len(columns))])
    return f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"


# def insert_data_to_db(df, conn, simulation_id):
#     """
#     Inserts the final dataframe into the AG_PP_OPTIMIZED_OUTPUT table.
#     Performs DELETE then INSERT for the simulation.
    
#     Parameters:
#         df: Final processed dataframe
#         conn: Oracle database connection
#         simulation_id: Simulation ID
#     """
#     try:
#         logger.info(f"Starting data insertion for simulation: {simulation_id}")
#         logger.info(f"DataFrame shape: {df.shape}")
#         logger.info(f"DataFrame columns: {list(df.columns)}")
        
#         cursor = conn.cursor()
#         table_name = "AG_PP_OPTIMIZED_OUTPUT"
        
#         # Define valid database columns (only columns that exist in the table)
#         valid_columns = [
#             "ROW_ID", "PLAN_ID", "PLAN_NAME", "PLAN_START_TIME", "SIMULATION_ID", 
#             "SIMULATION_NAME", "ORDER_NUMBER", "ORDER_LINE_NUMBER", "ELIGIBLE_LINE", 
#             "ORDER_TYPE", "ITEM_ID", "ITEM_DESCRIPTION", "ORGANIZATION_CODE", 
#             "DEPARTMENT_CODE", "FIXED_LOT_MULTIPLIER", "MINIMUM_ORDER_QUANTITY", 
#             "MAXIMUM_ORDER_QUANTITY", "UOM", "CONVERSION_FACTOR", "QUANTITY", 
#             "QUNATITY_CASE", "QUNATITY_PALLET", "QUNATITY_LITRE", "ORDER_DUE_DATE", 
#             "EARLIEST_START_DATE", "EXPIRY_DATE", "PRIORITY", "ABC_CLASSIFICATION", 
#             "CATEGORY_1", "CATEGORY_2", "CATEGORY_3", "CATEGORY_4", "CATEGORY_5", 
#             "CATEGORY_6", "CATEGORY_7", "CATEGORY_8", "CATEGORY_9", "CATEGORY_10", 
#             "CATEGORY_11", "CATEGORY_12", "CATEGORY_13", "CATEGORY_14", "CATEGORY_15", 
#             "CATEGORY_16", "CATEGORY_17", "CATEGORY_18", "CATEGORY_19", "CATEGORY_20", 
#             "WO_REFERENCE_NUMBER", "WO_RESOURCE_ID", "WO_START_TIME", "WO_END_TIME", 
#             "RESOURCE_CODE", "RUN_SPEED", "TOTAL_HOURS_FOR_QTY", "TOTAL_DAYS_FOR_QTY", 
#             "BLOCK_CODE", "BC_TASK_TYPE", "BC_START_TIME", "BC_END_TIME", "BC_SEQUENCE", 
#             "BC_QUANTITY_PRODUCED", "BC_QUANTITY_PRODUCED_CASE", "BC_QUANTITY_PRODUCED_PALLET", 
#             "BC_QUANTITY_PRODUCED_LITRE", "FINAL_TOTAL_HOURS_FOR_QTY", "FINAL_TOTAL_DAYS_FOR_QTY", 
#             "DATA_SUFFICIENT", "SCHEULED_IN_PLANNING_HORIZON", "PALLETS_QUANTITY", 
#             "LITRE_QUANTITY", "PALLETS_QUANTITY_PRODUCED", "LITRE_QUANTITY_PRODUCED"
#         ]
        
#         # Filter DataFrame to only include valid columns that exist in both df and valid_columns
#         columns_to_insert = [col for col in valid_columns if col in df.columns]
#         df_filtered = df[columns_to_insert].copy()
        
#         logger.info(f"Filtered to {len(columns_to_insert)} valid database columns")
#         logger.info(f"Columns being inserted: {columns_to_insert[:10]}... (showing first 10)")
        
#         # Convert DataFrame to list of dictionaries (only with valid columns)
#         data = [row_to_dict(row) for _, row in df_filtered.iterrows()]
#         logger.info(f"Converted DataFrame to list of {len(data)} dictionaries")
        
#         # Delete existing records for this simulation
#         delete_query = f"DELETE FROM AG_PP_OPTIMIZED_OUTPUT WHERE SIMULATION_ID = '{simulation_id}'"
#         cursor.execute(delete_query)
#         conn.commit()
#         logger.info(f"Deleted existing records for simulation {simulation_id}")
        
#         # Generate INSERT statements
#         insert_queries = generate_oracle_insert_statements(data, table_name)
#         logger.info(f"Generated {len(insert_queries)} insert statements")
        
#         # Execute INSERT statements
#         inserted_count = 0
#         for i, query in enumerate(insert_queries):
#             try:
#                 cursor.execute(query)
#                 conn.commit()
#                 inserted_count += 1
                
#                 # Log progress every 100 records
#                 if (i + 1) % 100 == 0:
#                     logger.info(f"Inserted {i + 1}/{len(insert_queries)} records...")
                    
#             except Exception as e:
#                 logger.error(f"Error executing insert query at index {i}: {e}")
#                 logger.error(f"Failed query: {query[:200]}...")  # Log first 200 chars
#                 conn.rollback()
#                 raise
        
#         logger.info(f"✅ Successfully inserted {inserted_count} records into {table_name}")
#         cursor.close()
#         return "Success"
        
#     except Exception as e:
#         logger.error(f"❌ Error inserting data to database: {e}", exc_info=True)
#         conn.rollback()
#         raise


def insert_data_to_db(df, conn, sim_id):
    # Convert DataFrame to list of dictionaries
    # conn,tunnel = get_oracle_connection()
    cursor = conn.cursor()
    table_name = "AG_PP_OPTIMIZED_OUTPUT"
    # batch_size = 1000


    logger.info(str(f"shape of the df ------------> {df.shape} "))
    data = [row_to_dict(row) for _, row in df.iterrows()]
    logger.info(str(f"Converted DataFrame to list of {len(data)} dictionaries"))

    truncate_query = f"delete from AG_PP_OPTIMIZED_OUTPUT where SIMULATION_ID='{sim_id}'"
    cursor.execute(truncate_query)
    conn.commit()

    insert_queries = generate_oracle_insert_statements(data, table_name)
    
    import json
    with open("iq.json", "w") as f:
        json.dump(insert_queries, f)

    try:
        for query in insert_queries:
            cursor.execute(query)
            conn.commit()
        logger.info("All insert queries executed successfully.")
        return "Success"
    except Exception as e:
        conn.rollback()  # Roll back the last uncommitted transaction
        logger.info(str(f"Error executing query:\n{query}\n\nException: {e}"))
        return f"Failed due to: {str(e)}"



# ============================================================================
# HELPER FUNCTION FOR WEEK BOUNDARIES
# ============================================================================

def _week_bounds_from_weekly_bucket(bucket_str):
    """
    Convert a week bucket string (WW-YYYY or YYYY-WW) to Sunday start and Saturday end datetime.
    
    Parameters:
        bucket_str: Week string in format "WW-YYYY" (e.g., "45-2025") or "YYYY-WW" (e.g., "2025-45")
        
    Returns:
        tuple: (start_datetime, end_datetime) for Sunday 00:00 to Saturday 23:59:59
    """
    if pd.isna(bucket_str) or not bucket_str:
        return None, None
    
    bucket_str = str(bucket_str).strip()
    
    # Try WW-YYYY format first
    if re.match(r'^\d{2}-\d{4}$', bucket_str):
        week, year = map(int, bucket_str.split('-'))
    # Try YYYY-WW format
    elif re.match(r'^\d{4}-\d{2}$', bucket_str):
        year, week = map(int, bucket_str.split('-'))
    else:
        logger.warning(f"Invalid week bucket format: {bucket_str}")
        return None, None
    
    # Calculate the first Sunday of the year
    jan1 = datetime(year, 1, 1)
    days_to_sunday = (6 - jan1.weekday()) % 7
    first_sunday = jan1 + timedelta(days=days_to_sunday)
    
    # Calculate the Sunday of the target week
    week_sunday = first_sunday + timedelta(weeks=week - 1)
    week_sunday = week_sunday.replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Saturday 23:59:59 of that week
    week_saturday = week_sunday + timedelta(days=6, hours=23, minutes=59, seconds=59)
    
    return week_sunday, week_saturday


# ============================================================================
# RESOURCE AVAILABILITY CALCULATION
# ============================================================================



def calculate_resource_availability(df_resource_availability, df_downtime, df_CIP_DOWNTIME):
    """
    Calculate weekly resource availability after all deductions.
    Main orchestration function that calls all sub-functions.
    
    Parameters:
    -----------
    df_resource_availability : pd.DataFrame
    df_downtime : pd.DataFrame
    df_CIP_DOWNTIME : pd.DataFrame
    
    Returns:
    --------
    pd.DataFrame : df_weekly_availability with AVAILABILITY column
    """
    
    # ==================== Supporting Functions ====================
    
    def find_unavailability_gaps(df_resource_availability):
        if df_resource_availability.empty:
            return pd.DataFrame(columns=[
                'ORGANIZATION_CODE', 'RESOURCE_CODE', 'DEPARTMENT_CODE',
                'UNAVAILABLE_FROM', 'UNAVAILABLE_TO', 'UNAVAILABLE_DAYS',
                'UNAVAILABLE_HOURS', 'GAP_AFTER_AVAILABILITY_RECORD',
                'GAP_BEFORE_AVAILABILITY_RECORD'
            ])     
        df = df_resource_availability.copy()
        df['FROM_TIME'] = pd.to_datetime(df['FROM_TIME'])
        df['TO_TIME'] = pd.to_datetime(df['TO_TIME'])
        
        grouping_cols = ['ORGANIZATION_CODE', 'RESOURCE_CODE']
        df_sorted = df.sort_values(grouping_cols + ['FROM_TIME'])
        
        unavailability_records = []
        now = pd.Timestamp.now() 
        for group_keys, group in df_sorted.groupby(grouping_cols):
            group = group.reset_index(drop=True)
            
            for i in range(len(group) - 1):
                current_to = group.loc[i, 'TO_TIME']
                next_from = group.loc[i + 1, 'FROM_TIME']
                
                if current_to < next_from:
                    unavailability_records.append({
                        'ORGANIZATION_CODE': group_keys[0],
                        'RESOURCE_CODE': group_keys[1],
                        'DEPARTMENT_CODE': group.loc[i, 'DEPARTMENT_CODE'],
                        'UNAVAILABLE_FROM': current_to,
                        'UNAVAILABLE_TO': next_from,
                        'UNAVAILABLE_DAYS': (next_from - current_to).days,
                        'UNAVAILABLE_HOURS': (next_from - current_to).total_seconds() / 3600,
                        'GAP_AFTER_AVAILABILITY_RECORD': group.loc[i, 'RECORD_NUM'],
                        'GAP_BEFORE_AVAILABILITY_RECORD': group.loc[i + 1, 'RECORD_NUM']
                    })

            week_start, _ = get_week_boundaries(now)
 
            unavailability_records.append({
                'ORGANIZATION_CODE': group_keys[0],
                'RESOURCE_CODE': group_keys[1],
                'DEPARTMENT_CODE': f"DEPT_{group_keys[1]}",
                'UNAVAILABLE_FROM': week_start,
                'UNAVAILABLE_TO': now,
                'UNAVAILABLE_DAYS': (now - week_start).days,
                'UNAVAILABLE_HOURS': (now - week_start).total_seconds() / 3600,
                'GAP_AFTER_AVAILABILITY_RECORD': unavailability_records[-1]['GAP_BEFORE_AVAILABILITY_RECORD'],
                'GAP_BEFORE_AVAILABILITY_RECORD': 0
            })
        
        return pd.DataFrame(unavailability_records)
    
    def get_week_boundaries(date):
        date = pd.to_datetime(date)
        jan1 = datetime(date.year, 1, 1)
        days_to_sunday = (6 - jan1.weekday()) % 7
        first_sunday = jan1 + timedelta(days=days_to_sunday)
        week_num = ((date - first_sunday).days // 7) + 1
        week_sunday = first_sunday + timedelta(weeks=week_num - 1)
        week_sunday = week_sunday.replace(hour=0, minute=0, second=0, microsecond=0)
        week_saturday = week_sunday + timedelta(days=6, hours=23, minutes=59, seconds=59)
        return week_sunday, week_saturday
    
    def add_weekly_aggregation(df_availability):
        if df_availability.empty:
            empty_cols = ['ORGANIZATION_CODE', 'RESOURCE_CODE', 'DEPARTMENT_CODE',
                          'WEEK_NUM', 'WEEK_START_DATE', 'WEEK_END_DATE',
                          'TOTAL_HOURS_AVAILABLE', 'TOTAL_CAPACITY_UNITS', 'DAYS_AVAILABLE',
                          'PLAN_ID', 'SIMULATION_ID', 'SIMULATION_NAME', 'PLAN_NAME', 'PLAN_START_TIME']
            return pd.DataFrame(), pd.DataFrame(columns=empty_cols)
        
        df = df_availability.copy()
        df['FROM_TIME'] = pd.to_datetime(df['FROM_TIME'])
        df['TO_TIME'] = pd.to_datetime(df['TO_TIME'])
        
        def get_sunday_week_num(date):
            if pd.isna(date):
                return None
            date = pd.to_datetime(date)
            # Find the Sunday of this date's week (days back to Sunday)
            weekday = date.weekday()  # Mon=0, Sun=6
            days_back = (weekday + 1) % 7
            sunday = date - timedelta(days=days_back)
            sunday = sunday.replace(hour=0, minute=0, second=0, microsecond=0)
            
            # Get ISO for the Sunday
            iso_year, iso_week, _ = sunday.isocalendar()
            
            return f"{iso_week:02d}-{iso_year}"
        
        df['WEEK_NUM'] = df['FROM_TIME'].apply(get_sunday_week_num)
        
        def get_week_boundaries(date):
            date = pd.to_datetime(date)
            jan1 = datetime(date.year, 1, 1)
            days_to_sunday = (6 - jan1.weekday()) % 7
            first_sunday = jan1 + timedelta(days=days_to_sunday)
            week_num = ((date - first_sunday).days // 7) + 1
            week_sunday = first_sunday + timedelta(weeks=week_num - 1)
            week_sunday = week_sunday.replace(hour=0, minute=0, second=0, microsecond=0)
            week_saturday = week_sunday + timedelta(days=6, hours=23, minutes=59, seconds=59)
            return week_sunday, week_saturday
        
        df[['WEEK_START_DATE', 'WEEK_END_DATE']] = df['FROM_TIME'].apply(
            lambda x: pd.Series(get_week_boundaries(x))
        )
        
        agg_dict = {
            'AVAILABILITY': 'sum',
            'PLAN_ID': 'first',
            'DEPARTMENT_CODE': 'first',
            'SIMULATION_ID': 'first',
            'SIMULATION_NAME': 'first',
            'PLAN_NAME': 'first',
            'PLAN_START_TIME': 'first',
            'CAPACITY_UNITS': 'sum',
            'RECORD_NUM': 'count'
        }
        
        df_weekly = df.groupby(
            ['ORGANIZATION_CODE', 'RESOURCE_CODE', 'WEEK_NUM']
        ).agg(agg_dict).reset_index()
        
        df_weekly = df_weekly.rename(columns={
            'AVAILABILITY': 'TOTAL_HOURS_AVAILABLE',
            'CAPACITY_UNITS': 'TOTAL_CAPACITY_UNITS',
            'RECORD_NUM': 'DAYS_AVAILABLE'
        })
        
        df_weekly['WEEK_START_DATE'] = df_weekly['WEEK_NUM'].apply(
            lambda wn: df[df['WEEK_NUM'] == wn]['WEEK_START_DATE'].iloc[0] if wn in df['WEEK_NUM'].values else None
        )
        df_weekly['WEEK_END_DATE'] = df_weekly['WEEK_NUM'].apply(
            lambda wn: df[df['WEEK_NUM'] == wn]['WEEK_END_DATE'].iloc[0] if wn in df['WEEK_NUM'].values else None
        )
        
        weekly_cols = [
            'ORGANIZATION_CODE', 'RESOURCE_CODE', 'DEPARTMENT_CODE',
            'WEEK_NUM', 'WEEK_START_DATE', 'WEEK_END_DATE',
            'TOTAL_HOURS_AVAILABLE', 'TOTAL_CAPACITY_UNITS', 'DAYS_AVAILABLE',
            'PLAN_ID', 'SIMULATION_ID', 'SIMULATION_NAME', 'PLAN_NAME', 'PLAN_START_TIME'
        ]
        df_weekly = df_weekly[weekly_cols]
        df_weekly = df_weekly.sort_values(['ORGANIZATION_CODE', 'RESOURCE_CODE', 'WEEK_START_DATE'])
        
        return df, df_weekly
    
    
    def add_weekly_unavailability_aggregation(df_unavailability):
        if df_unavailability.empty:
            return pd.DataFrame(), pd.DataFrame(columns=[
                'ORGANIZATION_CODE', 'RESOURCE_CODE', 'DEPARTMENT_CODE',
                'WEEK_NUM', 'WEEK_START_DATE', 'WEEK_END_DATE',
                'TOTAL_UNAVAILABLE_HOURS', 'NUM_UNAVAILABILITY_PERIODS'
            ])
        
        df = df_unavailability.copy()
        df['UNAVAILABLE_FROM'] = pd.to_datetime(df['UNAVAILABLE_FROM'])
        df['UNAVAILABLE_TO'] = pd.to_datetime(df['UNAVAILABLE_TO'])
        
        def get_sunday_week_num(date):
            if pd.isna(date):
                return None
            date = pd.to_datetime(date)
            # Find the Sunday of this date's week (days back to Sunday)
            weekday = date.weekday()  # Mon=0, Sun=6
            days_back = (weekday + 1) % 7
            sunday = date - timedelta(days=days_back)
            sunday = sunday.replace(hour=0, minute=0, second=0, microsecond=0)
            
            # Get ISO for the Sunday
            iso_year, iso_week, _ = sunday.isocalendar()
            
            return f"{iso_week:02d}-{iso_year}"

        def get_week_boundaries(date):
            date = pd.to_datetime(date)
            jan1 = datetime(date.year, 1, 1)
            days_to_sunday = (6 - jan1.weekday()) % 7
            first_sunday = jan1 + timedelta(days=days_to_sunday)
            week_num = ((date - first_sunday).days // 7) + 1
            week_sunday = first_sunday + timedelta(weeks=week_num - 1)
            week_sunday = week_sunday.replace(hour=0, minute=0, second=0, microsecond=0)
            week_saturday = week_sunday + timedelta(days=6, hours=23, minutes=59, seconds=59)
            return week_sunday, week_saturday
        
        split_records = []
        
        for idx, row in df.iterrows():
            unavail_from = row['UNAVAILABLE_FROM']
            unavail_to = row['UNAVAILABLE_TO']
            current_date = unavail_from
            
            while current_date < unavail_to:
                week_start, week_end = get_week_boundaries(current_date)
                week_num = get_sunday_week_num(current_date)
                
                period_start_in_week = max(unavail_from, week_start)
                period_end_in_week = min(unavail_to, week_end)
                hours_in_week = (period_end_in_week - period_start_in_week).total_seconds() / 3600
                
                split_record = {
                    'ORGANIZATION_CODE': row['ORGANIZATION_CODE'],
                    'RESOURCE_CODE': row['RESOURCE_CODE'],
                    'DEPARTMENT_CODE': row['DEPARTMENT_CODE'],
                    'WEEK_NUM': week_num,
                    'WEEK_START_DATE': week_start,
                    'WEEK_END_DATE': week_end,
                    'UNAVAILABLE_FROM_IN_WEEK': period_start_in_week,
                    'UNAVAILABLE_TO_IN_WEEK': period_end_in_week,
                    'UNAVAILABLE_HOURS_IN_WEEK': hours_in_week,
                    'ORIGINAL_UNAVAILABLE_FROM': unavail_from,
                    'ORIGINAL_UNAVAILABLE_TO': unavail_to,
                    'ORIGINAL_UNAVAILABLE_HOURS': row['UNAVAILABLE_HOURS'],
                    'GAP_AFTER_AVAILABILITY_RECORD': row['GAP_AFTER_AVAILABILITY_RECORD'],
                    'GAP_BEFORE_AVAILABILITY_RECORD': row['GAP_BEFORE_AVAILABILITY_RECORD']
                }
                
                split_records.append(split_record)
                current_date = week_end + timedelta(seconds=1)
        
        df_split = pd.DataFrame(split_records)
        
        df_weekly = df_split.groupby(
            ['ORGANIZATION_CODE', 'RESOURCE_CODE', 'DEPARTMENT_CODE', 'WEEK_NUM', 'WEEK_START_DATE', 'WEEK_END_DATE']
        ).agg(
            TOTAL_UNAVAILABLE_HOURS=('UNAVAILABLE_HOURS_IN_WEEK', 'sum'),
            NUM_UNAVAILABILITY_PERIODS=('UNAVAILABLE_HOURS_IN_WEEK', 'count')
        ).reset_index()
        
        df_weekly = df_weekly.sort_values(['ORGANIZATION_CODE', 'RESOURCE_CODE', 'WEEK_START_DATE'])
        df_split = df_split.sort_values(['ORGANIZATION_CODE', 'RESOURCE_CODE', 'WEEK_START_DATE', 'UNAVAILABLE_FROM_IN_WEEK'])
        
        return df_split, df_weekly
    
    def split_downtime_by_week(df_downtime):
        if df_downtime.empty:
            expected_cols = list(df_downtime.columns) if len(df_downtime.columns) > 0 else [
                'DOWNTIME_ID', 'ORGANIZATION_CODE', 'DEPARTMENT_CODE', 'RESOURCE_CODE',
                'DOWNTIME_CATEGORY', 'DOWNTIME_TYPE', 'DOWNTIME_DESCRIPTION',
                'FROM_DATETIME', 'TO_DATETIME', 'CAPACITY_UNITS', 'AVAILABILITY',
                'DELETED_FLAG', 'SIMULATION_ID', 'SIMULATION_NAME',
                'REC_RUN_START_TIME', 'REC_RUN_END_TIME', 'HOURS'
            ]
            additional_cols = ['WEEK_NUM', 'WEEK_START_DATE', 'WEEK_END_DATE', 
                              'FROM_DATETIME_IN_WEEK', 'TO_DATETIME_IN_WEEK', 'HOURS_IN_WEEK',
                              'ORIGINAL_FROM_DATETIME', 'ORIGINAL_TO_DATETIME', 'ORIGINAL_HOURS']
            return pd.DataFrame(columns=expected_cols + additional_cols)
        
        df = df_downtime.copy()
        df['FROM_DATETIME'] = pd.to_datetime(df['FROM_DATETIME'])
        df['TO_DATETIME'] = pd.to_datetime(df['TO_DATETIME'])
        
        def get_sunday_week_num(date):
            if pd.isna(date):
                return None
            date = pd.to_datetime(date)
            # Find the Sunday of this date's week (days back to Sunday)
            weekday = date.weekday()  # Mon=0, Sun=6
            days_back = (weekday + 1) % 7
            sunday = date - timedelta(days=days_back)
            sunday = sunday.replace(hour=0, minute=0, second=0, microsecond=0)
            
            # Get ISO for the Sunday
            iso_year, iso_week, _ = sunday.isocalendar()
            
            return f"{iso_week:02d}-{iso_year}"

        def get_week_boundaries(date):
            days_since_sunday = (date.weekday() + 1) % 7
            week_start = date - timedelta(days=days_since_sunday)
            week_start = week_start.replace(hour=0, minute=0, second=0, microsecond=0)
            week_end = week_start + timedelta(days=6, hours=23, minutes=59, seconds=59)
            return week_start, week_end
        
        split_records = []
        resource_downtime_map = {(rc,oc) : set() for rc, oc in itertools.product(list(df['RESOURCE_CODE'].unique()),list(df['ORGANIZATION_CODE'].unique()))}
        
        for idx, row in df.iterrows():
            from_dt = row['FROM_DATETIME']
            to_dt = row['TO_DATETIME']
            current_date = from_dt
            
            while current_date < to_dt:
                week_start, week_end = get_week_boundaries(current_date)
                week_num = get_sunday_week_num(current_date)
                
                period_start_in_week = max(from_dt, week_start)
                period_end_in_week = min(to_dt, week_end)
                prev_downtime = resource_downtime_map[(row['RESOURCE_CODE'], row['ORGANIZATION_CODE'])]
                if row['DOWNTIME_TYPE'] == 'Recurring' and not pd.isna(pd.to_datetime(row['REC_RUN_START_TIME'])) and not pd.isna(pd.to_datetime(row['REC_RUN_END_TIME'])):
                    # Works with 15min frequency
                    rng = pd.date_range(period_start_in_week, period_end_in_week, freq="15T", inclusive="left")
                    mask = (rng.time >= pd.to_datetime(row['REC_RUN_START_TIME']).time()) & (rng.time <  pd.to_datetime(row['REC_RUN_END_TIME']).time())
                    non_overlap_rng = set(rng[mask]) - prev_downtime
                else:
                    rng = set(pd.date_range(period_start_in_week, period_end_in_week, freq="15T", inclusive="left"))
                    non_overlap_rng = rng - prev_downtime

                hours_in_week = len(non_overlap_rng)/4
                resource_downtime_map[(row['RESOURCE_CODE'], row['ORGANIZATION_CODE'])] = prev_downtime.union(non_overlap_rng)

                split_record = row.to_dict()
                split_record.update({
                    'WEEK_NUM': week_num,
                    'WEEK_START_DATE': week_start,
                    'WEEK_END_DATE': week_end,
                    'FROM_DATETIME_IN_WEEK': period_start_in_week,
                    'TO_DATETIME_IN_WEEK': period_end_in_week,
                    'HOURS_IN_WEEK': hours_in_week,
                    'ORIGINAL_FROM_DATETIME': from_dt,
                    'ORIGINAL_TO_DATETIME': to_dt,
                    'ORIGINAL_HOURS': row['HOURS']
                })
                
                split_records.append(split_record)
                current_date = week_end + timedelta(seconds=1)
        
        df_split = pd.DataFrame(split_records)
        df_split = df_split.sort_values(['ORGANIZATION_CODE', 'RESOURCE_CODE', 'WEEK_START_DATE', 'FROM_DATETIME_IN_WEEK'])
        return df_split


    
    def calculate_cip_downtime(df_cip_downtime):
        if df_cip_downtime.empty:
            df_empty = df_cip_downtime.copy()
            df_empty['CIP_DOWN_FACTOR'] = pd.Series(dtype='float64')
            df_empty['CIP_DOWN_HRS'] = pd.Series(dtype='float64')
            return df_empty
        
        df = df_cip_downtime.copy()
        return df
    
    
    def calculate_weekly_availability(df_weekly_availability, df_weekly_unavailability, 
                                       df_downtime_split, df_CIP_DOWNTIME):
        df_result = df_weekly_availability.copy()
        # original_index = df_result.index

        # df_result['TOTAL_HOURS_AVAILABLE'] = 168.0
        df_result['TOTAL_HOURS_AVAILABLE'] = df_weekly_availability['TOTAL_HOURS_AVAILABLE']
        
        if not df_weekly_unavailability.empty:
            unavail_cols = ['ORGANIZATION_CODE', 'RESOURCE_CODE', 'DEPARTMENT_CODE', 'WEEK_NUM', 'TOTAL_UNAVAILABLE_HOURS']
            df_result = df_result.merge(
                df_weekly_unavailability[unavail_cols],
                on=['ORGANIZATION_CODE', 'RESOURCE_CODE', 'DEPARTMENT_CODE', 'WEEK_NUM'],
                how='left',
                suffixes=('', '_unavail')
            ).drop_duplicates()
            df_result['TOTAL_UNAVAILABLE_HOURS'] = df_result['TOTAL_UNAVAILABLE_HOURS'].fillna(0)
            df_result['TOTAL_HOURS_AVAILABLE'] = df_result['TOTAL_HOURS_AVAILABLE'] - df_result['TOTAL_UNAVAILABLE_HOURS']
            df_result = df_result.drop(columns=['TOTAL_UNAVAILABLE_HOURS'])
        
        if not df_downtime_split.empty:
            df_downtime_agg = df_downtime_split.groupby(
                ['ORGANIZATION_CODE', 'RESOURCE_CODE', 'DEPARTMENT_CODE', 'WEEK_NUM']
            )['HOURS_IN_WEEK'].sum().reset_index()
            
            df_result = df_result.merge(
                df_downtime_agg,
                on=['ORGANIZATION_CODE', 'RESOURCE_CODE', 'DEPARTMENT_CODE', 'WEEK_NUM'],
                how='left',
                suffixes=('', '_downtime')
            ).drop_duplicates()
            df_result['HOURS_IN_WEEK'] = df_result['HOURS_IN_WEEK'].fillna(0)
            df_result['TOTAL_HOURS_AVAILABLE'] = df_result['TOTAL_HOURS_AVAILABLE'] - df_result['HOURS_IN_WEEK']
            df_result = df_result.drop(columns=['HOURS_IN_WEEK'])
        
        if not df_CIP_DOWNTIME.empty:
            df_result = df_result.merge(
                df_CIP_DOWNTIME,
                on=['ORGANIZATION_CODE', 'RESOURCE_CODE', 'DEPARTMENT_CODE'],
                how='left',
                suffixes=('', '_cip')
            ).drop_duplicates()
            df_result['MAXIMUM_DURATION'] = df_result['MAXIMUM_DURATION'].fillna(0)
            df_result['CIP_DOWN_FACTOR'] = df_result.apply(
                lambda row: int(row['TOTAL_HOURS_AVAILABLE'] / row['MAXIMUM_DURATION']) if row['MAXIMUM_DURATION'] != 0 else 0, 
                axis=1
            )
            df_result['CIP_DOWN_HRS'] = df_result['CIP_DOWN_FACTOR']*df_result['DOWNTIME_IN_HRS']
            df_result['TOTAL_HOURS_AVAILABLE'] = df_result['TOTAL_HOURS_AVAILABLE'] - df_result['CIP_DOWN_HRS']
        
        df_result['TOTAL_HOURS_AVAILABLE'] = df_result['TOTAL_HOURS_AVAILABLE'].clip(lower=0)
        # df_result.index = original_index
        normal_expected = 152  # Adjust based on your standard (7 days - average deductions)
        df_result['TOTAL_HOURS_AVAILABLE'] = df_result['TOTAL_HOURS_AVAILABLE'].clip(upper=normal_expected)
        logger.info("Capped gapped weeks at normal expected ~152h to avoid over 164h in chart")
        return df_result
    
    # ==================== Main Logic ====================
    
    logger.info("Starting resource availability calculation")

    # plan_start_time = datetime.now()
    logger.info(f"Plan execution time (using system time):")
    
    df_resource_availability['FROM_TIME'] = pd.to_datetime(df_resource_availability['FROM_TIME'], errors='coerce')
    df_resource_availability['TO_TIME'] = pd.to_datetime(df_resource_availability['TO_TIME'], errors='coerce')
    # # Filter out rows that end before or at the plan start time
    # df_resource_availability = df_resource_availability[df_resource_availability['TO_TIME'] > df_resource_availability['PLAN_START_TIME']]

    # # Rows where availability spans the plan start time (current day availability)
    # df_resource_availability1 = df_resource_availability[(df_resource_availability['FROM_TIME'] < df_resource_availability['PLAN_START_TIME']) & (df_resource_availability['TO_TIME'] > df_resource_availability['PLAN_START_TIME'])]

    # for idx, row in df_resource_availability1.iterrows():
    #     f = row.get('FROM_TIME') 
    #     t = row.get('TO_TIME')
    #     if pd.isna(f) or pd.isna(t):
    #         continue
    #     # Calculate remaining availability from plan start time to end of day
    #     new_avail = max(0.0, (t - row['PLAN_START_TIME']).total_seconds() / 3600.0)
    #     if 'AVAILABILITY' in df_resource_availability1.columns:
    #         df_resource_availability1.at[idx, 'AVAILABILITY'] = new_avail
    #     # Set FROM_TIME to plan start time (current system time)
    #     df_resource_availability1.at[idx, 'FROM_TIME'] = row['PLAN_START_TIME']

    # # Combine: future days + current day (adjusted to plan start time)
    # df_resource_availability = pd.concat([df_resource_availability[df_resource_availability['FROM_TIME'] >= row['PLAN_START_TIME']], df_resource_availability1],ignore_index=True)

    ###30-12 logic start
    logger.info("Applying updated availability logic for current week (include past days in week + prorate current day)")

    plan_start = df_resource_availability['PLAN_START_TIME'].iloc[0]  # Current date/time (Dec 30, 2025)

    # Calculate the Sunday start of the current week
    days_to_sunday = (plan_start.weekday() + 1) % 7
    week_start = plan_start - timedelta(days=days_to_sunday)
    week_start = week_start.replace(hour=0, minute=0, second=0, microsecond=0)

    week_end = week_start + timedelta(days=6, hours=23, minutes=59, seconds=59)
    logger.info(f"Current week for availability: {week_start.date()} to {week_end.date()}")

    # Keep all rows that overlap the current week
    df_current_week = df_resource_availability[
        (df_resource_availability['TO_TIME'] > week_start) &
        (df_resource_availability['FROM_TIME'] < week_end + timedelta(days=1))
    ].copy()

    # Prorate the current day (Dec 30) to remaining hours only
    current_day_mask = (df_current_week['FROM_TIME'].dt.date == plan_start.date())
    if current_day_mask.any():
        idx = df_current_week[current_day_mask].index[0]
        original_avail = df_current_week.loc[idx, 'AVAILABILITY']
        day_end = df_current_week.loc[idx, 'TO_TIME']
        remaining_hours = max(0.0, (day_end - plan_start).total_seconds() / 3600.0)
        
        df_current_week.loc[idx, 'AVAILABILITY'] = remaining_hours
        df_current_week.loc[idx, 'FROM_TIME'] = plan_start  # Optional: shift start to now
        
        logger.info(f"Prorated current day ({plan_start.date()}): {original_avail:.1f}h → {remaining_hours:.1f}h remaining")

    # Keep all future weeks (starting after current week) unchanged
    df_future_weeks = df_resource_availability[
        df_resource_availability['FROM_TIME'] >= week_end + timedelta(days=1)
    ].copy()

    # Combine current week (with past + prorated) + future full weeks
    df_resource_availability = pd.concat([df_current_week, df_future_weeks], ignore_index=True)
    df_resource_availability = df_resource_availability.sort_values('FROM_TIME').reset_index(drop=True)

    logger.info("Updated availability applied: current week includes past days + prorated current + future full")
    # === END OF UPDATED LOGIC ===


    df_unavailability = find_unavailability_gaps(df_resource_availability)
    logger.info(f"Found {len(df_unavailability)} unavailability gaps")
    
    df_availability_with_week, df_weekly_availability = add_weekly_aggregation(df_resource_availability)
    logger.info(f"Created {len(df_weekly_availability)} weekly availability records")
    
    df_unavailability_split, df_weekly_unavailability = add_weekly_unavailability_aggregation(df_unavailability)
    logger.info(f"Created {len(df_weekly_unavailability)} weekly unavailability records")
    
    if df_downtime is not None and not df_downtime.empty:
        if 'HOURS' not in df_downtime.columns:
            df_downtime = df_downtime.copy()
            df_downtime['HOURS'] = (pd.to_datetime(df_downtime['TO_DATETIME']) - pd.to_datetime(df_downtime['FROM_DATETIME'])).dt.total_seconds() / 3600
        df_downtime_split = split_downtime_by_week(df_downtime)
        logger.info(f"Split downtime into {len(df_downtime_split)} weekly records")
    else:
        df_downtime_split = pd.DataFrame()
        logger.info("No downtime data to process")
    
    if df_CIP_DOWNTIME is not None and not df_CIP_DOWNTIME.empty:
        df_cip_processed = calculate_cip_downtime(df_CIP_DOWNTIME)
        logger.info(f"Calculated CIP downtime for {len(df_cip_processed)} records")
    else:
        df_cip_processed = pd.DataFrame()
        logger.info("No CIP downtime data to process")
    
    df_result = calculate_weekly_availability(df_weekly_availability, df_weekly_unavailability, df_downtime_split, df_cip_processed)
    logger.info(f"Calculated final weekly availability for {len(df_result)} records")
    
    df_result = df_result.rename(columns={
        'TOTAL_HOURS_AVAILABLE': 'AVAILABILITY',
        'WEEK_NUM': 'YEAR_WEEK'
    })

    df_result.to_csv("extracted_weekly_availability.csv", index=False)
    
    # Store these for later use
    df_result._weekly_unavailability = df_weekly_unavailability
    df_result._downtime_split = df_downtime_split
    df_result._cip_downtime = df_cip_processed
    
    return df_result






# ============================================================================
# WEEK NUMBER FUNCTIONS
# ============================================================================

def add_week_num(df, date_column='START_DATETIME', week_column_name='WEEK_NUM'):
    """
    Adds a week number column based on the specified date column.
    Week starts on Sunday and ends on Saturday 23:59.
    
    Parameters:
    -----------
    df : pd.DataFrame
        Input dataframe
    date_column : str, default='START_DATETIME'
        Name of the column containing dates to convert
    week_column_name : str, default='WEEK_NUM'
        Name of the output week number column
    
    Returns:
    --------
    pd.DataFrame
        Dataframe with added week number column (format: "WW-YYYY")
    """
    
    if df.empty:
        df_result = df.copy()
        df_result[week_column_name] = pd.Series(dtype='str')
        return df_result
    
    if date_column not in df.columns:
        raise ValueError(f"Column '{date_column}' not found in dataframe. Available columns: {list(df.columns)}")
    
    df_result = df.copy()
    
    def get_sunday_week_num(date):
            if pd.isna(date):
                return None
            date = pd.to_datetime(date)
            # Find the Sunday of this date's week (days back to Sunday)
            weekday = date.weekday()  # Mon=0, Sun=6
            days_back = (weekday + 1) % 7
            sunday = date - timedelta(days=days_back)
            sunday = sunday.replace(hour=0, minute=0, second=0, microsecond=0)
            
            # Get ISO for the Sunday
            iso_year, iso_week, _ = sunday.isocalendar()
            
            return f"{iso_week:02d}-{iso_year}"

    df_result[week_column_name] = df_result[date_column].apply(get_sunday_week_num)
    
    invalid_weeks = df_result[df_result[week_column_name].isna() & df_result[date_column].notna()]
    if not invalid_weeks.empty:
        logger.warning(f"Could not compute week numbers for {len(invalid_weeks)} rows with valid dates in '{date_column}'")
    
    return df_result


# ============================================================================
# RUN RULE AND FLAG FUNCTIONS
# ============================================================================

def add_run_rule_flag(df_input, df_run_rules):
    """
    Adds a RUN_RULE_FLAG column to df_input based on matching ITEM_ID, ORGANIZATION_CODE,
    RESOURCE_CODE, and WEEK_NUM with df_run_rules.
    """
    if not isinstance(df_input, pd.DataFrame) or not isinstance(df_run_rules, pd.DataFrame):
        raise ValueError("Both 'df_input' and 'df_run_rules' must be pandas DataFrames.")
    
    required_input_cols = ['ITEM_ID', 'ORGANIZATION_CODE', 'RESOURCE_CODE', 'WEEK_NUM']
    missing_input_cols = [col for col in required_input_cols if col not in df_input.columns]
    if missing_input_cols:
        raise ValueError(f"Missing columns in df_input: {missing_input_cols}")

    required_rules_cols = ['ITEM', 'ORGANIZATION_CODE', 'RESOURCE_CODE', 'WEEK_NUM']
    missing_rules_cols = [col for col in required_rules_cols if col not in df_run_rules.columns]
    if missing_rules_cols:
        raise ValueError(f"Missing columns in df_run_rules: {missing_rules_cols}")

    df_input_copy = df_input.copy()
    df_merged = df_input_copy.merge(
        df_run_rules[['ITEM', 'ORGANIZATION_CODE', 'RESOURCE_CODE', 'WEEK_NUM']],
        left_on=['ITEM_ID', 'ORGANIZATION_CODE', 'RESOURCE_CODE', 'WEEK_NUM'],
        right_on=['ITEM', 'ORGANIZATION_CODE', 'RESOURCE_CODE', 'WEEK_NUM'],
        how='left',
        indicator=True
    )
    df_input_copy['RUN_RULE_FLAG'] = df_merged['_merge'].map({'both': 'Y', 'left_only': 'N'})
    
    logger.info(f"Run rule flag added: {(df_input_copy['RUN_RULE_FLAG'] == 'Y').sum()} matches found")
    return df_input_copy


def add_expiry_flag(df):
    """
    Adds an EXPIRY_FLAG column to the DataFrame based on the presence of valid dates in EXPIRY_DATE.
    """
    if not isinstance(df, pd.DataFrame):
        raise ValueError("Input 'df' must be a pandas DataFrame.")
    if 'EXPIRY_DATE' not in df.columns:
        raise ValueError("Column 'EXPIRY_DATE' not found in DataFrame.")

    df_copy = df.copy()
    df_copy['EXPIRY_DATE'] = pd.to_datetime(df_copy['EXPIRY_DATE'], errors='coerce')
    df_copy['EXPIRY_FLAG'] = df_copy['EXPIRY_DATE'].notna().map({True: 'Y', False: 'N'})

    logger.info(f"Expiry flag added: {(df_copy['EXPIRY_FLAG'] == 'Y').sum()} items with expiry dates")
    return df_copy


def sort_input_by_priority(df):
    """
    Copies df_input and sorts by priority levels based on RUN_RULE_FLAG and EXPIRY_FLAG.
    """
    if not isinstance(df, pd.DataFrame):
        raise ValueError("Input 'df' must be a pandas DataFrame.")
    required_cols = ['RUN_RULE_FLAG', 'EXPIRY_FLAG', 'EXPIRY_DATE', 'ORDER_DUE_DATE']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing columns: {missing_cols}")

    output_df = df.copy()
    output_df['EXPIRY_DATE'] = pd.to_datetime(output_df['EXPIRY_DATE'], errors='coerce')
    output_df['ORDER_DUE_DATE'] = pd.to_datetime(output_df['ORDER_DUE_DATE'], format='%d-%m-%Y', errors='coerce')

    def assign_priority(row):
        if row['RUN_RULE_FLAG'] == 'Y' and row['EXPIRY_FLAG'] == 'Y':
            return 1
        elif row['RUN_RULE_FLAG'] == 'Y' and row['EXPIRY_FLAG'] == 'N':
            return 2
        elif row['RUN_RULE_FLAG'] == 'N' and row['EXPIRY_FLAG'] == 'Y':
            return 3
        else:
            return 4

    output_df['priority'] = output_df.apply(assign_priority, axis=1)
    output_df = output_df.sort_values(
        by=['priority', 'EXPIRY_DATE', 'ORDER_DUE_DATE'],
        na_position='last'
    )
    output_df = output_df.drop(columns=['priority'])
    output_df = output_df.reset_index(drop=True)
    
    logger.info("Input sorted by priority")
    return output_df


# ============================================================================
# WEEKLY BUCKET ASSIGNMENT
# ============================================================================


def fix_invalid_week_num(output_df):
    """
    Fix invalid WEEK_NUM values (like '00-2026') by recalculating from available date fields.
    Priority: ORDER_DUE_DATE > EARLIEST_START_DATE > Current date
    
    Args:
        output_df: DataFrame with WEEK_NUM, ORDER_DUE_DATE, EARLIEST_START_DATE columns
    
    Returns:
        output_df: DataFrame with corrected WEEK_NUM values
        fix_summary: Dictionary with fix statistics
    """
    
    def calculate_week_num_from_date(date_value):
        """
        Calculate WEEK_NUM in 'WW-YYYY' format from a date.
        Uses ISO week date system where week 1 is the first week with Thursday in the new year.
        """
        if pd.isna(date_value):
            return None
        
        try:
            dt = pd.to_datetime(date_value)
            
            # Use ISO calendar for accurate week calculation
            iso_year, iso_week, iso_weekday = dt.isocalendar()
            
            # Handle edge case: early January dates might belong to previous year's week 52/53
            # and late December dates might belong to next year's week 1
            if dt.month == 1 and iso_week >= 52:
                # Early January but belongs to previous year
                year = iso_year
            elif dt.month == 12 and iso_week == 1:
                # Late December but belongs to next year
                year = iso_year
            else:
                year = iso_year
            
            return f"{iso_week:02d}-{year}"
        
        except Exception as e:
            logger.error(f"Error calculating week number from date {date_value}: {e}")
            return None
    
    # Track statistics
    fix_summary = {
        'total_invalid': 0,
        'fixed_from_due_date': 0,
        'fixed_from_earliest_start': 0,
        'fixed_from_current_date': 0,
        'still_invalid': 0,
        'fixed_orders': []
    }
    
    # Find all invalid WEEK_NUM entries
    invalid_mask = (
        output_df['WEEK_NUM'].isna() | 
        output_df['WEEK_NUM'].str.startswith('00-', na=False) |
        output_df['WEEK_NUM'].str.startswith('0-', na=False) |
        ~output_df['WEEK_NUM'].str.match(r'^\d{2}-\d{4}$', na=False)
    )
    
    fix_summary['total_invalid'] = invalid_mask.sum()
    
    if fix_summary['total_invalid'] == 0:
        logger.info("No invalid WEEK_NUM values found")
        return output_df, fix_summary
    
    logger.warning(f"Found {fix_summary['total_invalid']} orders with invalid WEEK_NUM")
    
    # Process each invalid entry
    for idx in output_df[invalid_mask].index:
        original_week = output_df.at[idx, 'WEEK_NUM']
        new_week = None
        fix_method = None
        
        # Priority 1: Try ORDER_DUE_DATE
        due_date = pd.to_datetime(output_df.at[idx, 'ORDER_DUE_DATE'], errors='coerce')
        if pd.notna(due_date):
            new_week = calculate_week_num_from_date(due_date)
            if new_week:
                fix_method = 'ORDER_DUE_DATE'
                fix_summary['fixed_from_due_date'] += 1
        
        # Priority 2: Try EARLIEST_START_DATE
        if not new_week:
            earliest_start = pd.to_datetime(output_df.at[idx, 'EARLIEST_START_DATE'], errors='coerce')
            if pd.notna(earliest_start):
                new_week = calculate_week_num_from_date(earliest_start)
                if new_week:
                    fix_method = 'EARLIEST_START_DATE'
                    fix_summary['fixed_from_earliest_start'] += 1
        
        # Priority 3: Use current date as last resort
        if not new_week:
            current_date = datetime.now()
            new_week = calculate_week_num_from_date(current_date)
            if new_week:
                fix_method = 'CURRENT_DATE'
                fix_summary['fixed_from_current_date'] += 1
                logger.warning(f"Order at index {idx}: Using current date as fallback for WEEK_NUM")
        
        # Apply fix or mark as still invalid
        if new_week:
            output_df.at[idx, 'WEEK_NUM'] = new_week
            fix_summary['fixed_orders'].append({
                'index': idx,
                'original': original_week,
                'new': new_week,
                'method': fix_method
            })
            logger.info(f"Fixed order {idx}: '{original_week}' -> '{new_week}' (using {fix_method})")
        else:
            fix_summary['still_invalid'] += 1
            logger.error(f"Order at index {idx}: Cannot fix WEEK_NUM - no valid dates available")
    
    # Log summary
    logger.info("=" * 60)
    logger.info("WEEK_NUM Fix Summary:")
    logger.info(f"  Total invalid entries found: {fix_summary['total_invalid']}")
    logger.info(f"  Fixed from ORDER_DUE_DATE: {fix_summary['fixed_from_due_date']}")
    logger.info(f"  Fixed from EARLIEST_START_DATE: {fix_summary['fixed_from_earliest_start']}")
    logger.info(f"  Fixed from CURRENT_DATE (fallback): {fix_summary['fixed_from_current_date']}")
    logger.info(f"  Still invalid (no dates available): {fix_summary['still_invalid']}")
    logger.info("=" * 60)
    
    return output_df, fix_summary


def validate_week_num_format(output_df):
    """
    Validate that all WEEK_NUM values are in correct 'WW-YYYY' format.
    
    Returns:
        bool: True if all valid, False otherwise
        list: List of invalid entries with details
    """
    invalid_entries = []
    
    for idx, row in output_df.iterrows():
        week_num = row['WEEK_NUM']
        
        # Check if null
        if pd.isna(week_num):
            invalid_entries.append({
                'index': idx,
                'week_num': week_num,
                'reason': 'NULL value'
            })
            continue
        
        # Check format
        if not isinstance(week_num, str):
            invalid_entries.append({
                'index': idx,
                'week_num': week_num,
                'reason': 'Not a string'
            })
            continue
        
        # Check pattern WW-YYYY
        if not week_num.count('-') == 1:
            invalid_entries.append({
                'index': idx,
                'week_num': week_num,
                'reason': 'Invalid format (missing or extra hyphen)'
            })
            continue
        
        parts = week_num.split('-')
        if len(parts) != 2:
            invalid_entries.append({
                'index': idx,
                'week_num': week_num,
                'reason': 'Invalid format (cannot split into week-year)'
            })
            continue
        
        try:
            week = int(parts[0])
            year = int(parts[1])
            
            # Validate week range (1-53)
            if week < 1 or week > 53:
                invalid_entries.append({
                    'index': idx,
                    'week_num': week_num,
                    'reason': f'Week number {week} out of range (1-53)'
                })
            
            # Validate year range (reasonable range)
            if year < 2020 or year > 2030:
                invalid_entries.append({
                    'index': idx,
                    'week_num': week_num,
                    'reason': f'Year {year} out of reasonable range (2020-2030)'
                })
        
        except ValueError:
            invalid_entries.append({
                'index': idx,
                'week_num': week_num,
                'reason': 'Week or year is not a number'
            })
    
    is_valid = len(invalid_entries) == 0
    return is_valid, invalid_entries

def assign_weekly_bucket(output_df, df_run_rules, df_resource_availability, df_change_over_matrix, 
                         resource_unavailibility_df=None, normal_downtime_df=None):
    """
    Assigns weekly_bucket to output_df in two phases: first work orders, then non-work orders,
    sorted by priority. Tracks running sum of TOTAL_HOURS_FOR_QTY and changeover time per
    ORGANIZATION_CODE, RESOURCE_CODE, YEAR_WEEK to ensure total does not exceed available hours.
    """
    
    logger.info("Starting weekly bucket assignment")
    
    ## 08-12 START ADDITION    
    # ===== ADD THIS SECTION AT THE START =====
    # Fix invalid WEEK_NUM values before processing
    # logger.info("Checking for invalid WEEK_NUM values...")
    # output_df, fix_summary = fix_invalid_week_num(output_df)
    
    # Validate all WEEK_NUM are now correct
    # is_valid, invalid_entries = validate_week_num_format(output_df)
    # if not is_valid:
    #     logger.error(f"Still have {len(invalid_entries)} invalid WEEK_NUM entries after fix attempt:")
    #     for entry in invalid_entries[:10]:  # Show first 10
    #         logger.error(f"  Index {entry['index']}: {entry['week_num']} - {entry['reason']}")

    ## 08-12 END ADDITION

    output_df['WEEKLY_BUCKET'] = None
    weekly_usage = {}
    schedule = {}
    
    def week_end_date(week_str):
        """Return week Saturday datetime from WW-YYYY format."""
        if pd.isna(week_str) or not isinstance(week_str, str):
            return None
        try:
            parts = week_str.split('-')
            week, year = int(parts[0]), int(parts[1])
            jan1 = datetime(year, 1, 1)
            days_to_sunday = (6 - jan1.weekday()) % 7
            first_sunday = jan1 + timedelta(days=days_to_sunday)
            week_sunday = first_sunday + timedelta(weeks=week - 1)
            week_saturday = week_sunday + timedelta(days=6, hours=23, minutes=59, seconds=59)
            return week_saturday
        except:
            return None
    
    def advance_week(week_str, k):
        """Advance week by k weeks."""
        if pd.isna(week_str) or not isinstance(week_str, str):
            return None
        try:
            parts = week_str.split('-')
            week, year = int(parts[0]), int(parts[1])
        except:
            return None
        
        week += k
        while week > 52:
            week -= 52
            year += 1
        while week < 1:
            week += 52
            year -= 1
        
        return f"{week:02d}-{year}"

    def last_item_in_week(org, res, week):
        """Get the last item_id for tasks in the week for specific org/resource."""
        key = (org, res, week)
        if key in schedule and schedule[key]:
            for entry in reversed(schedule[key]):
                if entry.get('TYPE') == 'TASK':
                    return entry.get('ITEM_ID')
        return None

    def get_changeover_hours(prev_item, to_item, org, dept, res):
        """Get changeover hours; default 1 hour if no match or invalid."""
        if pd.isna(prev_item) or pd.isna(to_item):
            return 1.0, "No prev/to item; default 1h", True
        
        mask = (
            (df_change_over_matrix['ORGANIZATION_CODE'] == org) &
            (df_change_over_matrix['DEPARTMENT_CODE'] == dept) &
            (df_change_over_matrix['RESOURCE_CODE'] == res) &
            (df_change_over_matrix['FROM_ITEM'] == prev_item) &
            (df_change_over_matrix['TO_ITEM'] == to_item)
        )
        matches = df_change_over_matrix[mask]
        
        if not matches.empty and pd.notna(matches['DURATION'].iloc[0]):
            duration = float(matches['DURATION'].iloc[0])
            if duration < 0:
                return 1.0, "Negative duration; default 1h", False
            return duration, None, True
        
        return 1.0, "No changeover match; default 1h", True

    def try_assign_week(org, res, dept, item, hours, week, esd):
        """Attempt to assign task to week, respecting availability, downtime, and ESD."""
        
        # Check for full downtime/unavailability
        if resource_unavailibility_df is not None and not resource_unavailibility_df.empty:
            unavail_check = resource_unavailibility_df[
                (resource_unavailibility_df['ORGANIZATION_CODE'] == org) &
                (resource_unavailibility_df['RESOURCE_CODE'] == res) &
                (resource_unavailibility_df['YEAR_WEEK'] == week) &
                (resource_unavailibility_df['UNAVAILABILITY'] >= 168)
            ]
            if not unavail_check.empty:
                return False, 0.0
        
        if normal_downtime_df is not None and not normal_downtime_df.empty:
            downtime_check = normal_downtime_df[
                (normal_downtime_df['ORGANIZATION_CODE'] == org) &
                (normal_downtime_df['RESOURCE_CODE'] == res) &
                (normal_downtime_df['YEAR_WEEK'] == week) &
                (normal_downtime_df['UNAVAILABILITY'] >= 168)
            ]
            if not downtime_check.empty:
                return False, 0.0

        # Check availability
        row = df_resource_availability[
            (df_resource_availability['ORGANIZATION_CODE'] == org) &
            (df_resource_availability['RESOURCE_CODE'] == res) &
            (df_resource_availability['YEAR_WEEK'] == week)
        ]
        
        if row.empty or float(row['AVAILABILITY'].iloc[0]) <= 0:
            return False, 0.0

        # Check ESD
        we = week_end_date(week)
        if pd.notna(esd) and we is not None and not (we > esd):
            return False, 0.0

        # Get current usage
        key = (org, res, week)
        used = weekly_usage.get(key, 0.0)
        avail = float(row['AVAILABILITY'].iloc[0])

        # Calculate changeover
        prev_item = last_item_in_week(org, res, week)
        co_hours, co_msg, co_valid = get_changeover_hours(prev_item, item, org, dept, res)

        # Check if fits
        total_needed = hours + co_hours
        if total_needed > avail:
            return False, 0.0

        # Assign
        df_resource_availability.loc[row.index, 'AVAILABILITY'] = avail - total_needed
        weekly_usage[key] = used + total_needed
        
        if key not in schedule:
            schedule[key] = []
        if co_hours > 0:
            schedule[key].append({'TYPE': 'CHANGEOVER', 'ITEM_ID': item, 'DURATION': float(co_hours)})
        schedule[key].append({'TYPE': 'TASK', 'ITEM_ID': item, 'DURATION': float(hours)})
        
        return True, co_hours

    def sort_by_priority(df):
        """Sort by priority levels based on RUN_RULE_FLAG and EXPIRY_FLAG."""
        if df.empty:
            return df
        
        output_df_temp = df.copy()
        
        def assign_priority(row):
            if row['RUN_RULE_FLAG'] == 'Y' and row['EXPIRY_FLAG'] == 'Y':
                return 1
            elif row['RUN_RULE_FLAG'] == 'Y' and row['EXPIRY_FLAG'] == 'N':
                return 2
            elif row['RUN_RULE_FLAG'] == 'N' and row['EXPIRY_FLAG'] == 'Y':
                return 3
            else:
                return 4

        output_df_temp['priority'] = output_df_temp.apply(assign_priority, axis=1)
        output_df_temp = output_df_temp.sort_values(
            by=['priority', 'EXPIRY_DATE', 'ORDER_DUE_DATE'],
            na_position='last'
        )
        output_df_temp = output_df_temp.drop(columns=['priority'])
        return output_df_temp

    # Phase 1: Work Orders
    work_orders = output_df[output_df['ORDER_TYPE'] == 'Work order'].copy()
    if not work_orders.empty:
        work_orders = sort_by_priority(work_orders)
        logger.info(f"Processing {len(work_orders)} work orders")
        
        for idx, row in work_orders.iterrows():
            org = row['ORGANIZATION_CODE']
            res = row['RESOURCE_CODE']
            dept = row['DEPARTMENT_CODE']
            item = row['ITEM_ID']
            hours = float(row['TOTAL_HOURS_FOR_QTY'])
            due_week = row['WEEK_NUM']
            esd = pd.to_datetime(row['EARLIEST_START_DATE'], errors='coerce')
            priority = (
                1 if row['RUN_RULE_FLAG'] == 'Y' and row['EXPIRY_FLAG'] == 'Y' else
                2 if row['RUN_RULE_FLAG'] == 'Y' else
                3 if row['EXPIRY_FLAG'] == 'Y' else 4
            )

            if pd.isna(due_week) or not isinstance(due_week, str):
                output_df.at[idx, 'CP_REMARKS'] = f"Priority {priority}: Unassigned: invalid WEEK_NUM"
                continue
            
            if hours <= 0:
                output_df.at[idx, 'CP_REMARKS'] = f"Priority {priority}: Unassigned: invalid TOTAL_HOURS_FOR_QTY"
                continue

            # Get all allowed rule weeks, sorted
            assigned = False
            rr = (
                df_run_rules
                .loc[
                    (df_run_rules['ORGANIZATION_CODE'] == org) &
                    (df_run_rules['RESOURCE_CODE'] == res) &
                    (df_run_rules['ITEM'] == item)
                ]
                .assign(WEEK_NEW=lambda d: d['WEEK_NUM'].str[3:] + '-' + d['WEEK_NUM'].str[:2])
                .sort_values('WEEK_NEW')
            )
            
            if not rr.empty:
                allowed_weeks = sorted(rr['WEEK_NUM'].unique())  # Sorted rule weeks
                
                # Filter to weeks >= due_week if possible
                due_week_dt = week_end_date(due_week)
                if due_week_dt:
                    allowed_weeks = [w for w in allowed_weeks if week_end_date(w) >= due_week_dt]
                
                # Try each allowed week in order
                for wk in allowed_weeks:
                    success, co_hours = try_assign_week(org, res, dept, item, hours, wk, esd)
                    if success:
                        output_df.at[idx, 'WEEKLY_BUCKET'] = wk
                        output_df.at[idx, 'OUTPUT_CHANGEOVER'] = co_hours
                        output_df.at[idx, 'CP_REMARKS'] = f"Priority {priority}: Allocated to {wk} (run rule)"
                        assigned = True
                        break
                
                if not assigned:
                    output_df.at[idx, 'CP_REMARKS'] = f"Priority {priority}: Unassigned: no capacity in any run rule week"
            else:
                # No run rules: try due_week only
                success, co_hours = try_assign_week(org, res, dept, item, hours, due_week, esd)
                if success:
                    output_df.at[idx, 'WEEKLY_BUCKET'] = due_week
                    output_df.at[idx, 'OUTPUT_CHANGEOVER'] = co_hours
                    output_df.at[idx, 'CP_REMARKS'] = f"Priority {priority}: Allocated to {due_week}"
                    assigned = True

            if not assigned and rr.empty:
                output_df.at[idx, 'CP_REMARKS'] = f"Priority {priority}: Unassigned: no feasible week (ESD/availability/downtime)"

    # Phase 2: Non-Work Orders
    non_work_orders = output_df[output_df['ORDER_TYPE'] != 'Work order'].copy()
    if not non_work_orders.empty:
        non_work_orders = sort_by_priority(non_work_orders)
        logger.info(f"Processing {len(non_work_orders)} non-work orders")

        for idx, row in non_work_orders.iterrows():
            org = row['ORGANIZATION_CODE']
            res = row['RESOURCE_CODE']
            dept = row['DEPARTMENT_CODE']
            item = row['ITEM_ID']
            hours = float(row['TOTAL_HOURS_FOR_QTY'])
            due_week = row['WEEK_NUM']
            esd = pd.to_datetime(row['EARLIEST_START_DATE'], errors='coerce')
            priority = (
                1 if row['RUN_RULE_FLAG'] == 'Y' and row['EXPIRY_FLAG'] == 'Y' else
                2 if row['RUN_RULE_FLAG'] == 'Y' else
                3 if row['EXPIRY_FLAG'] == 'Y' else 4
            )

            if pd.isna(due_week) or not isinstance(due_week, str):
                output_df.at[idx, 'CP_REMARKS'] = f"Priority {priority}: Unassigned: invalid WEEK_NUM"
                continue
            
            if hours <= 0:
                output_df.at[idx, 'CP_REMARKS'] = f"Priority {priority}: Unassigned: invalid TOTAL_HOURS_FOR_QTY"
                continue

            assigned = False
            rr = (
                df_run_rules
                .loc[
                    (df_run_rules['ORGANIZATION_CODE'] == org) &
                    (df_run_rules['RESOURCE_CODE'] == res) &
                    (df_run_rules['ITEM'] == item)
                ]
                .assign(WEEK_NEW=lambda d: d['WEEK_NUM'].str[3:] + '-' + d['WEEK_NUM'].str[:2])
                .sort_values('WEEK_NEW')
            )
            
            if not rr.empty:
                allowed_weeks = sorted(rr['WEEK_NUM'].unique())
                
                due_week_dt = week_end_date(due_week)
                if due_week_dt:
                    allowed_weeks = [w for w in allowed_weeks if week_end_date(w) >= due_week_dt]
                
                for wk in allowed_weeks:
                    success, co_hours = try_assign_week(org, res, dept, item, hours, wk, esd)
                    if success:
                        output_df.at[idx, 'WEEKLY_BUCKET'] = wk
                        output_df.at[idx, 'OUTPUT_CHANGEOVER'] = co_hours
                        output_df.at[idx, 'CP_REMARKS'] = f"Priority {priority}: Allocated to {wk} (run rule)"
                        assigned = True
                        break
                
                if not assigned:
                    output_df.at[idx, 'CP_REMARKS'] = f"Priority {priority}: Unassigned: no capacity in any run rule week"
            else:
                success, co_hours = try_assign_week(org, res, dept, item, hours, due_week, esd)
                if success:
                    output_df.at[idx, 'WEEKLY_BUCKET'] = due_week
                    output_df.at[idx, 'OUTPUT_CHANGEOVER'] = co_hours
                    output_df.at[idx, 'CP_REMARKS'] = f"Priority {priority}: Allocated to {due_week}"
                    assigned = True

            if not assigned and rr.empty:
                output_df.at[idx, 'CP_REMARKS'] = f"Priority {priority}: Unassigned: no feasible week (ESD/availability/downtime)"

    assigned_count = output_df['WEEKLY_BUCKET'].notna().sum()
    logger.info(f"Weekly bucket assignment complete: {assigned_count}/{len(output_df)} orders assigned")
    
    return output_df, df_resource_availability


# ============================================================================
# BC TIME CALCULATION
# ============================================================================

def calculate_bc_times(df):
    """
    Calculates BC_START_TIME and BC_END_TIME from WEEKLY_BUCKET and sets BC_TASK_TYPE.
    
    Handles multiple input formats:
    1. "WW-YYYY" (e.g., "45-2025") - Week format
    2. "YYYY-WW" (e.g., "2025-45") - Week format
    3. "YYYY-MM-DD" - Converts to week
    4. datetime objects - Converts to week
    5. None/NaN/Empty - Returns None values
    """
    
    if not isinstance(df, pd.DataFrame):
        raise ValueError("Input must be a pandas DataFrame.")
    
    if 'WEEKLY_BUCKET' not in df.columns:
        raise ValueError("Column 'WEEKLY_BUCKET' not found in DataFrame.")
    
    logger.info("Calculating BC times from WEEKLY_BUCKET")
    
    result_df = df.copy()
    
    def convert_to_week_format(value):
        """Convert any input to YYYY-WW week format."""
        if pd.isna(value):
            return None
        
        if isinstance(value, str):
            value = value.strip()
            if not value:
                return None
        
        # Case 1: Already in week format "WW-YYYY" (e.g., "45-2025")
        if isinstance(value, str) and re.match(r'^\d{2}-\d{4}$', value):
            try:
                week, year = map(int, value.split('-'))
                if 1 <= week <= 53:
                    return f"{year}-{week:02d}"
            except ValueError:
                return None
        
        # Case 2: Already in week format "YYYY-WW" (e.g., "2025-45")
        if isinstance(value, str) and re.match(r'^\d{4}-\d{2}$', value):
            try:
                year, week = map(int, value.split('-'))
                if 1 <= week <= 53:
                    return value
            except ValueError:
                return None
        
        # Case 3-5: Date strings or datetime objects
        try:
            date = pd.to_datetime(value)
            jan1 = datetime(date.year, 1, 1)
            days_to_sunday = (6 - jan1.weekday()) % 7
            first_sunday = jan1 + timedelta(days=days_to_sunday)
            week_num = ((date - first_sunday).days // 7) + 1
            return f"{date.year}-{week_num:02d}"
        except:
            return None
    
    def week_to_date_range(week_str):
        """Convert YYYY-WW format to (Sunday 00:00:00, Saturday 23:59:59)."""
        if pd.isna(week_str):
            return None, None
        
        try:
            year, week = map(int, week_str.split('-'))
            jan1 = datetime(year, 1, 1)
            days_to_sunday = (6 - jan1.weekday()) % 7
            first_sunday = jan1 + timedelta(days=days_to_sunday)
            week_sunday = first_sunday + timedelta(weeks=week - 1)
            week_sunday = week_sunday.replace(hour=0, minute=0, second=0, microsecond=0)
            week_saturday = week_sunday + timedelta(days=6, hours=23, minutes=59, seconds=59)
            return week_sunday, week_saturday
        except:
            return None, None
    
    # Step 1: Standardize WEEKLY_BUCKET
    result_df['_STANDARDIZED_WEEK'] = result_df['WEEKLY_BUCKET'].apply(convert_to_week_format)
    
    # Step 2: Calculate BC times
    date_ranges = result_df['_STANDARDIZED_WEEK'].apply(week_to_date_range)
    result_df['BC_START_TIME'] = date_ranges.apply(lambda x: x[0])
    result_df['BC_END_TIME'] = date_ranges.apply(lambda x: x[1])
    
    # Step 3: Set BC_TASK_TYPE
    result_df['BC_TASK_TYPE'] = result_df['BC_START_TIME'].apply(
        lambda x: 'TASK' if pd.notna(x) else None
    )
    
    # Step 4: Clean up temporary column
    result_df = result_df.drop(columns=['_STANDARDIZED_WEEK'])
    
    # Log statistics
    total = len(result_df)
    assigned = len(result_df[result_df['BC_TASK_TYPE'] == 'TASK'])
    unassigned = total - assigned
    logger.info(f"BC times calculation complete: {assigned}/{total} assigned ({assigned/total*100:.1f}%), {unassigned} unassigned")
    
    return result_df


# ============================================================================
# CHANGEOVER TASK ADDITION
# ============================================================================

def add_changeover_task(output_df):
    """
    Adds changeover summary rows for each resource per WEEK_NUM.
    
    For each unique combination of (ORGANIZATION_CODE, RESOURCE_CODE, WEEKLY_BUCKET):
    - Creates ONE new row with BC_TASK_TYPE = 'CHANGEOVER'
    - Sums all OUTPUT_CHANGEOVER hours for that resource/week
    """
    
    if not isinstance(output_df, pd.DataFrame):
        raise ValueError("Input must be a pandas DataFrame.")
    
    logger.info("Adding changeover summary tasks")
    
    required_cols = [
        'OUTPUT_CHANGEOVER', 'BC_START_TIME', 'BC_END_TIME', 'BC_TASK_TYPE',
        'ORGANIZATION_CODE', 'RESOURCE_CODE', 'WEEKLY_BUCKET',
        'PLAN_ID', 'PLAN_NAME', 'SIMULATION_ID', 'SIMULATION_NAME', 'DEPARTMENT_CODE'
    ]
    
    missing_cols = [col for col in required_cols if col not in output_df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    result_df = output_df.copy()
    
    # Filter only rows with valid BC_TASK_TYPE = 'TASK'
    task_rows = result_df[result_df['BC_TASK_TYPE'] == 'TASK'].copy()
    
    if task_rows.empty:
        logger.warning("No tasks with BC_TASK_TYPE='TASK' found. No changeover rows to add.")
        return result_df
    
    # Filter out rows with None/NaN in WEEKLY_BUCKET
    task_rows = task_rows[task_rows['WEEKLY_BUCKET'].notna()].copy()
    
    if task_rows.empty:
        logger.warning("No tasks with valid WEEKLY_BUCKET found. No changeover rows to add.")
        return result_df
    
    # Aggregate changeover hours per resource per week
    changeover_summary = task_rows.groupby(
        ['ORGANIZATION_CODE', 'RESOURCE_CODE', 'WEEKLY_BUCKET', 'DEPARTMENT_CODE'], 
        dropna=False
    ).agg({
        'OUTPUT_CHANGEOVER': 'sum',
        'BC_START_TIME': 'first',
        'BC_END_TIME': 'first',
        'PLAN_ID': 'first',
        'PLAN_NAME': 'first',
        'SIMULATION_ID': 'first',
        'SIMULATION_NAME': 'first'
    }).reset_index()
    
    # Filter only groups that have changeover hours > 0
    changeover_summary = changeover_summary[changeover_summary['OUTPUT_CHANGEOVER'] > 0]
    
    if changeover_summary.empty:
        logger.info("No changeover hours found. No changeover rows to add.")
        return result_df
    
    # Create changeover task rows
    changeover_rows = []
    
    for idx, row in changeover_summary.iterrows():
        changeover_row = {
            'ORGANIZATION_CODE': row['ORGANIZATION_CODE'],
            'RESOURCE_CODE': row['RESOURCE_CODE'],
            'DEPARTMENT_CODE': row['DEPARTMENT_CODE'],
            'WEEKLY_BUCKET': row['WEEKLY_BUCKET'],
            'PLAN_ID': row['PLAN_ID'],
            'PLAN_NAME': row['PLAN_NAME'],
            'SIMULATION_ID': row['SIMULATION_ID'],
            'SIMULATION_NAME': row['SIMULATION_NAME'],
            'BC_START_TIME': row['BC_START_TIME'],
            'BC_END_TIME': row['BC_END_TIME'],
            'BC_TASK_TYPE': 'CHANGEOVER',
            'OUTPUT_CHANGEOVER': row['OUTPUT_CHANGEOVER'],
            'ROW_ID': None,
            'ORDER_NUMBER': f"CHANGEOVER_{row['ORGANIZATION_CODE']}_{row['RESOURCE_CODE']}_{row['WEEKLY_BUCKET']}",
            'ORDER_TYPE': 'CHANGEOVER',
            'ITEM_ID': None,
            'QUANTITY': None,
            'TOTAL_HOURS_FOR_QTY': row['OUTPUT_CHANGEOVER'],
            'CP_REMARKS': f"Changeover summary for {row['RESOURCE_CODE']} in week {row['WEEKLY_BUCKET']}: {row['OUTPUT_CHANGEOVER']:.2f} hours"
        }
        
        changeover_rows.append(changeover_row)
    
    changeover_df = pd.DataFrame(changeover_rows)
    
    # Align columns with result_df
    for col in result_df.columns:
        if col not in changeover_df.columns:
            changeover_df[col] = None
    
    changeover_df = changeover_df[result_df.columns]
    
    result_df = pd.concat([result_df, changeover_df], ignore_index=True)
    
    logger.info(f"Added {len(changeover_df)} changeover summary rows")
    logger.info(f"Total rows after adding changeover tasks: {len(result_df)}")
    
    return result_df


# ============================================================================
# CIP DOWNTIME ADDITION
# ============================================================================

def add_CIP_downtime(output_df, df_CIP_DOWNTIME, df_weekly_availability):
    """
    Adds CIP downtime rows for each resource per week where the resource matches.
    """
    
    if not isinstance(output_df, pd.DataFrame):
        raise ValueError("Input 'output_df' must be a pandas DataFrame.")
    
    if not isinstance(df_CIP_DOWNTIME, pd.DataFrame):
        raise ValueError("Input 'df_CIP_DOWNTIME' must be a pandas DataFrame.")
    
    logger.info("Adding CIP downtime rows")
    
    required_cols_output = [
        'ORGANIZATION_CODE', 'RESOURCE_CODE', 'WEEKLY_BUCKET',
        'BC_START_TIME', 'BC_END_TIME', 'BC_TASK_TYPE',
        'PLAN_ID', 'PLAN_NAME', 'SIMULATION_ID', 'SIMULATION_NAME', 'DEPARTMENT_CODE'
    ]
    
    missing_cols = [col for col in required_cols_output if col not in output_df.columns]
    if missing_cols:
        raise ValueError(f"Missing columns in output_df: {missing_cols}")
    
    required_cols_cip = ['RESOURCE_CODE', 'CIP_DOWN_HRS']
    missing_cols_cip = [col for col in required_cols_cip if col not in df_weekly_availability.columns]
    if missing_cols_cip:
        raise ValueError(f"Missing columns in df_CIP_DOWNTIME: {missing_cols_cip}")
    
    result_df = output_df.copy()
    df_cip = df_CIP_DOWNTIME.copy()
    df_weekly_avail = df_weekly_availability.copy()

    df_weekly_avail = df_weekly_avail[df_weekly_avail['CIP_DOWN_HRS'].notna()].copy()
    df_weekly_avail = df_weekly_avail[df_weekly_avail['CIP_DOWN_HRS'] > 0].copy()
    
    # Filter only rows with valid BC_TASK_TYPE = 'TASK'
    task_rows = result_df[result_df['BC_TASK_TYPE'] == 'TASK'].copy()
    
    if task_rows.empty:
        logger.warning("No tasks with BC_TASK_TYPE='TASK' found. Cannot determine week boundaries for CIP downtime.")
        return result_df
    
    task_rows = task_rows[task_rows['WEEKLY_BUCKET'].notna()].copy()
    
    if task_rows.empty:
        logger.warning("No tasks with valid WEEKLY_BUCKET found. Cannot add CIP downtime rows.")
        return result_df

    
    if df_cip.empty:
        logger.info("No CIP downtime hours found. No CIP downtime rows to add.")
        return result_df
    
    # Get unique resource-week combinations
    resource_week_metadata = task_rows.groupby(
        ['ORGANIZATION_CODE', 'RESOURCE_CODE', 'WEEKLY_BUCKET', 'DEPARTMENT_CODE'],
        dropna=False
    ).agg({
        'BC_START_TIME': 'first',
        'BC_END_TIME': 'first',
        'PLAN_ID': 'first',
        'PLAN_NAME': 'first',
        'SIMULATION_ID': 'first',
        'SIMULATION_NAME': 'first'
    }).reset_index()
    # Merge with CIP downtime data on RESOURCE_CODE only
    cip_matches = resource_week_metadata.merge(
        df_weekly_avail[['RESOURCE_CODE', 'YEAR_WEEK', 'CIP_DOWN_HRS']],
        left_on=['RESOURCE_CODE', 'WEEKLY_BUCKET'],
        right_on=['RESOURCE_CODE', 'YEAR_WEEK'],
        how='inner'
    )
    
    if cip_matches.empty:
        logger.info("No matching resources found between output_df and df_CIP_DOWNTIME.")
        return result_df
    
    # Create CIP downtime rows
    cip_rows = []
    
    for idx, row in cip_matches.iterrows():
        cip_row = {
            'ORGANIZATION_CODE': row['ORGANIZATION_CODE'],
            'RESOURCE_CODE': row['RESOURCE_CODE'],
            'DEPARTMENT_CODE': row['DEPARTMENT_CODE'],
            'WEEKLY_BUCKET': row['WEEKLY_BUCKET'],
            'PLAN_ID': row['PLAN_ID'],
            'PLAN_NAME': row['PLAN_NAME'],
            'SIMULATION_ID': row['SIMULATION_ID'],
            'SIMULATION_NAME': row['SIMULATION_NAME'],
            'BC_START_TIME': row['BC_START_TIME'],
            'BC_END_TIME': row['BC_END_TIME'],
            'BC_TASK_TYPE': 'CIP_DOWNTIME',
            'TOTAL_HOURS_FOR_QTY': row['CIP_DOWN_HRS'],
            'OUTPUT_CHANGEOVER': 0.0,
            'ROW_ID': None,
            'ORDER_NUMBER': f"CIP_DOWNTIME_{row['ORGANIZATION_CODE']}_{row['RESOURCE_CODE']}_{row['WEEKLY_BUCKET']}",
            'ORDER_TYPE': 'CIP_DOWNTIME',
            'ITEM_ID': None,
            'QUANTITY': None,
            'CP_REMARKS': f"CIP downtime for {row['RESOURCE_CODE']} in week {row['WEEKLY_BUCKET']}: {row['CIP_DOWN_HRS']:.2f} hours"
        }
        
        cip_rows.append(cip_row)
    
    cip_df = pd.DataFrame(cip_rows)
    
    # Align columns with result_df
    for col in result_df.columns:
        if col not in cip_df.columns:
            cip_df[col] = None
    
    cip_df = cip_df[result_df.columns]
    
    result_df = pd.concat([result_df, cip_df], ignore_index=True)
    
    logger.info(f"Added {len(cip_df)} CIP downtime rows")
    logger.info(f"Total rows after adding CIP downtime: {len(result_df)}")
    
    return result_df


# ============================================================================
# UNAVAILABILITY AND DOWNTIME ADDITION
# ============================================================================
from datetime import datetime, timedelta
import pandas as pd

def _week_bounds_from_weekly_bucket(bucket: str):
    """
    Convert a weekly bucket string (format: 'WW-YYYY') to start and end datetime.
    Week starts on Sunday and ends on Saturday.
    
    Args:
        bucket: String in format 'WW-YYYY' (e.g., '01-2025')
    
    Returns:
        tuple: (start_datetime, end_datetime)
    """
    week_num, year = bucket.split('-')
    week_num = int(week_num)
    year = int(year)
    
    # Find the first Sunday of the year
    jan1 = datetime(year, 1, 1)
    days_to_sunday = (6 - jan1.weekday()) % 7
    first_sunday = jan1 + timedelta(days=days_to_sunday)
    
    # Calculate the start of the requested week
    if week_num == 0:
        # Week 0 is before the first Sunday
        week_start = datetime(year, 1, 1)
        week_end = first_sunday - timedelta(seconds=1)
    else:
        week_start = first_sunday + timedelta(weeks=week_num - 1)
        week_end = week_start + timedelta(days=6, hours=23, minutes=59, seconds=59)
    
    return week_start, week_end


def add_unavailability_from_weekly_df(output_df, df_weekly_unavailability):
    """
    Append RESOURCE_UNAVAILABLE rows using df_weekly_unavailability.
    - For weeks already in output_df: copy metadata and add unavailability
    - For weeks only in unavailability_df: add unavailability with minimal metadata from source
    """
    if df_weekly_unavailability.empty:
        logger.info("df_weekly_unavailability is empty → no rows added")
        return output_df.copy()

    logger.info("Adding resource unavailability rows")
    
    # Prepare
    df = df_weekly_unavailability.copy()
    if 'WEEK_NUM' in df.columns:
        df = df.rename(columns={'WEEK_NUM': 'WEEKLY_BUCKET'})
    df['WEEKLY_BUCKET'] = df['WEEKLY_BUCKET'].astype(str)

    # Use only RESOURCE_UNAVAILABLE
    summary_df = df.copy()
    summary_df['BC_TASK_TYPE'] = 'RESOURCE_UNAVAILABLE'

    # Week bounds (Sunday → Saturday)
    def get_week_bounds(bucket):
        start, end = _week_bounds_from_weekly_bucket(bucket)
        return pd.Series([start, end], index=['BC_START_TIME', 'BC_END_TIME'])
    
    summary_df[['BC_START_TIME', 'BC_END_TIME']] = summary_df['WEEKLY_BUCKET'].apply(get_week_bounds)

    # Hours
    summary_df['TOTAL_HOURS_FOR_QTY'] = summary_df['TOTAL_UNAVAILABLE_HOURS'].round(2)
    summary_df['TOTAL_DAYS_FOR_QTY'] = (summary_df['TOTAL_UNAVAILABLE_HOURS'] / 24).round(2)

    # Track which unavailability records we've processed
    processed_unavail = set()
    
    # PART 1: Add unavailability for weeks ALREADY in output_df (with metadata)
    grouped = output_df.groupby(['ORGANIZATION_CODE', 'RESOURCE_CODE', 'WEEKLY_BUCKET'])
    final_rows = []

    for (org, res, bucket), group in grouped:
        if group.empty:
            continue

        meta = {
            'PLAN_ID': group['PLAN_ID'].dropna().iloc[0] if 'PLAN_ID' in group.columns and not group['PLAN_ID'].dropna().empty else None,
            'PLAN_NAME': group['PLAN_NAME'].dropna().iloc[0] if 'PLAN_NAME' in group.columns and not group['PLAN_NAME'].dropna().empty else None,
            'PLAN_START_TIME': group['PLAN_START_TIME'].dropna().iloc[0] if 'PLAN_START_TIME' in group.columns and not group['PLAN_START_TIME'].dropna().empty else None,
            'SIMULATION_ID': group['SIMULATION_ID'].dropna().iloc[0] if 'SIMULATION_ID' in group.columns and not group['SIMULATION_ID'].dropna().empty else None,
            'SIMULATION_NAME': group['SIMULATION_NAME'].dropna().iloc[0] if 'SIMULATION_NAME' in group.columns and not group['SIMULATION_NAME'].dropna().empty else None,
            'DEPARTMENT_CODE': group['DEPARTMENT_CODE'].dropna().iloc[0] if 'DEPARTMENT_CODE' in group.columns and not group['DEPARTMENT_CODE'].dropna().empty else None,
        }

        unavail = summary_df[
            (summary_df['ORGANIZATION_CODE'] == org) &
            (summary_df['RESOURCE_CODE'] == res) &
            (summary_df['WEEKLY_BUCKET'] == bucket)
        ]

        for idx, row in unavail.iterrows():
            # Mark this unavailability as processed
            processed_unavail.add((org, res, bucket, idx))
            
            new_row = {col: None for col in output_df.columns}
            new_row.update(meta)
            new_row.update({
                'ORGANIZATION_CODE': org,
                'RESOURCE_CODE': res,
                'WEEKLY_BUCKET': bucket,
                'BC_TASK_TYPE': 'RESOURCE_UNAVAILABLE',
                'BC_START_TIME': row['BC_START_TIME'],
                'BC_END_TIME': row['BC_END_TIME'],
                'TOTAL_HOURS_FOR_QTY': row['TOTAL_HOURS_FOR_QTY'],
                'TOTAL_DAYS_FOR_QTY': row['TOTAL_DAYS_FOR_QTY'],
                'OUTPUT_CHANGEOVER': 0.0,
                'ORDER_TYPE': 'RESOURCE_UNAVAILABLE',
                'ORDER_NUMBER': f"UNAVAILABLE_{org}_{res}_{bucket}",
                'CP_REMARKS': f"Resource unavailability for {res} in week {bucket}: {row['TOTAL_HOURS_FOR_QTY']:.2f} hours",
            })
            final_rows.append(new_row)

    # PART 2: Add unavailability for weeks NOT in output_df (get metadata from summary_df)
    for idx, row in summary_df.iterrows():
        org = row['ORGANIZATION_CODE']
        res = row['RESOURCE_CODE']
        bucket = row['WEEKLY_BUCKET']
        
        # Skip if already processed in Part 1
        if (org, res, bucket, idx) in processed_unavail:
            continue
            
        # This is a NEW week not in output_df - get metadata from summary_df itself
        new_row = {col: None for col in output_df.columns}
        new_row.update({
            'ORGANIZATION_CODE': org,
            'RESOURCE_CODE': res,
            'WEEKLY_BUCKET': bucket,
            'BC_TASK_TYPE': 'RESOURCE_UNAVAILABLE',
            'BC_START_TIME': row['BC_START_TIME'],
            'BC_END_TIME': row['BC_END_TIME'],
            'TOTAL_HOURS_FOR_QTY': row['TOTAL_HOURS_FOR_QTY'],
            'TOTAL_DAYS_FOR_QTY': row['TOTAL_DAYS_FOR_QTY'],
            'OUTPUT_CHANGEOVER': 0.0,
            'ORDER_TYPE': 'RESOURCE_UNAVAILABLE',
            'ORDER_NUMBER': f"UNAVAILABLE_{org}_{res}_{bucket}",
            'CP_REMARKS': f"Resource unavailability for {res} in week {bucket}: {row['TOTAL_HOURS_FOR_QTY']:.2f} hours (new week)",
            # Get metadata from the source dataframe
            'PLAN_ID': row.get('PLAN_ID'),
            'PLAN_NAME': row.get('PLAN_NAME'),
            'PLAN_START_TIME': row.get('PLAN_START_TIME'),
            'SIMULATION_ID': row.get('SIMULATION_ID'),
            'SIMULATION_NAME': row.get('SIMULATION_NAME'),
            'DEPARTMENT_CODE': row.get('DEPARTMENT_CODE'),
        })
        final_rows.append(new_row)

    if not final_rows:
        logger.info("No unavailability to add")
        return output_df.copy()

    final_summary = pd.DataFrame(final_rows)
    result = pd.concat([output_df, final_summary], ignore_index=True)
    
    existing_weeks = len([r for r in final_rows if r['PLAN_ID'] is not None])
    new_weeks = len(final_rows) - existing_weeks
    logger.info(f"Added {len(final_summary)} RESOURCE_UNAVAILABLE rows ({existing_weeks} for existing weeks, {new_weeks} for new weeks)")
    
    return result


def add_downtime_from_split(output_df, df_downtime_split):
    """
    Append DOWNTIME summary rows using df_downtime_split.
    - For weeks already in output_df: copy metadata and add downtime
    - For weeks only in downtime_split: add downtime with minimal metadata from source
    Uses HOURS_IN_WEEK from df_downtime_split.
    """
    if df_downtime_split.empty:
        logger.info("df_downtime_split is empty → no DOWNTIME rows added")
        return output_df.copy()

    logger.info("Adding downtime rows from split data")
    
    # Normalise week format to WW-YYYY
    out = output_df.copy()
    out['WEEKLY_BUCKET'] = out['WEEKLY_BUCKET'].astype(str) \
        .str.replace(r'^(\d{4})-(\d{2})$', r'\2-\1', regex=True)

    dt = df_downtime_split.copy()
    dt['WEEK_NUM'] = dt['WEEK_NUM'].astype(str)
    dt['WEEKLY_BUCKET'] = dt['WEEK_NUM'].str.replace(r'^(\d{4})-(\d{2})$', r'\2-\1', regex=True)

    # Aggregate HOURS_IN_WEEK per resource/week, keeping metadata columns
    agg_dict = {
        'HOURS_IN_WEEK': 'sum'
    }
    
    # Add metadata columns to aggregation if they exist
    metadata_cols = ['SIMULATION_ID', 'SIMULATION_NAME', 'DEPARTMENT_CODE']
    for col in metadata_cols:
        if col in dt.columns:
            agg_dict[col] = 'first'
    
    agg = dt.groupby(['ORGANIZATION_CODE', 'RESOURCE_CODE', 'WEEKLY_BUCKET']).agg(agg_dict).reset_index()
    agg = agg.rename(columns={'HOURS_IN_WEEK': 'DOWNTIME_HOURS'})
    agg['DOWNTIME_HOURS'] = agg['DOWNTIME_HOURS'].round(2)
    
    # Track which downtime records we've processed
    processed_downtime = set()

    # PART 1: Add downtime for weeks ALREADY in output_df (with metadata)
    grouped = out.groupby(['ORGANIZATION_CODE', 'RESOURCE_CODE', 'WEEKLY_BUCKET'])
    summary_rows = []

    for (org, res, bucket), group in grouped:
        if group.empty:
            continue

        meta = {
            col: group[col].dropna().iloc[0]
            if col in group.columns and not group[col].dropna().empty else None
            for col in ['PLAN_ID', 'PLAN_NAME', 'PLAN_START_TIME',
                        'SIMULATION_ID', 'SIMULATION_NAME', 'DEPARTMENT_CODE']
        }

        downtime = agg[
            (agg['ORGANIZATION_CODE'] == org) &
            (agg['RESOURCE_CODE'] == res) &
            (agg['WEEKLY_BUCKET'] == bucket)
        ]

        if downtime.empty or downtime['DOWNTIME_HOURS'].iloc[0] <= 0:
            continue

        # Mark this downtime as processed
        processed_downtime.add((org, res, bucket))
        
        hours = downtime['DOWNTIME_HOURS'].iloc[0]
        start, end = _week_bounds_from_weekly_bucket(bucket)

        row = {col: None for col in out.columns}
        row.update(meta)
        row.update({
            'ORGANIZATION_CODE': org,
            'RESOURCE_CODE': res,
            'WEEKLY_BUCKET': bucket,
            'BC_TASK_TYPE': 'DOWNTIME',
            'BC_START_TIME': start,
            'BC_END_TIME': end,
            'TOTAL_HOURS_FOR_QTY': hours,
            'TOTAL_DAYS_FOR_QTY': round(hours / 24, 2),
            'OUTPUT_CHANGEOVER': 0.0,
            'ORDER_TYPE': 'DOWNTIME',
            'ORDER_NUMBER': f"DOWNTIME_{org}_{res}_{bucket}",
            'CP_REMARKS': f"DOWNTIME for {res} in week {bucket}: {hours:.2f} hours",
        })
        summary_rows.append(row)

    # PART 2: Add downtime for weeks NOT in output_df (get metadata from agg)
    for _, dt_row in agg.iterrows():
        org = dt_row['ORGANIZATION_CODE']
        res = dt_row['RESOURCE_CODE']
        bucket = dt_row['WEEKLY_BUCKET']
        hours = dt_row['DOWNTIME_HOURS']
        
        # Skip if already processed in Part 1 or if hours <= 0
        if (org, res, bucket) in processed_downtime or hours <= 0:
            continue
        
        # This is a NEW week not in output_df - get metadata from agg (downtime source)
        start, end = _week_bounds_from_weekly_bucket(bucket)
        
        new_row = {col: None for col in out.columns}
        new_row.update({
            'ORGANIZATION_CODE': org,
            'RESOURCE_CODE': res,
            'WEEKLY_BUCKET': bucket,
            'BC_TASK_TYPE': 'DOWNTIME',
            'BC_START_TIME': start,
            'BC_END_TIME': end,
            'TOTAL_HOURS_FOR_QTY': hours,
            'TOTAL_DAYS_FOR_QTY': round(hours / 24, 2),
            'OUTPUT_CHANGEOVER': 0.0,
            'ORDER_TYPE': 'DOWNTIME',
            'ORDER_NUMBER': f"DOWNTIME_{org}_{res}_{bucket}",
            'CP_REMARKS': f"DOWNTIME for {res} in week {bucket}: {hours:.2f} hours (new week)",
            # Get metadata from the source dataframe
            'SIMULATION_ID': dt_row.get('SIMULATION_ID'),
            'SIMULATION_NAME': dt_row.get('SIMULATION_NAME'),
            'DEPARTMENT_CODE': dt_row.get('DEPARTMENT_CODE'),
            'PLAN_ID': None,
            'PLAN_NAME': None,
            'PLAN_START_TIME': None,
        })
        summary_rows.append(new_row)

    if not summary_rows:
        logger.info("No DOWNTIME rows added")
        return out

    result = pd.concat([out, pd.DataFrame(summary_rows)], ignore_index=True)
    
    existing_weeks = len([r for r in summary_rows if r['PLAN_ID'] is not None])
    new_weeks = len(summary_rows) - existing_weeks
    logger.info(f"Added {len(summary_rows)} DOWNTIME rows ({existing_weeks} for existing weeks, {new_weeks} for new weeks)")
    
    return result

# ============================================================================
# FINAL OUTPUT PROCESSING
# ============================================================================

def finalize_output(output_df):
    """
    Finalizes the output dataframe with required columns and transformations.
    Only includes columns that exist in the database table.
    """
    logger.info("Finalizing output dataframe")
    
    # Add final columns
    
    output_df['FINAL_TOTAL_HOURS_FOR_QTY'] = output_df['TOTAL_HOURS_FOR_QTY']
    output_df['FINAL_TOTAL_DAYS_FOR_QTY'] = (output_df['TOTAL_HOURS_FOR_QTY']/24)
    
    # Extract year from WEEKLY_BUCKET (format is "WW-YYYY", we want "YYYY")
    output_df['WEEKLY_BUCKET'] = output_df['WEEKLY_BUCKET'].str.split('-').str[0]
    output_df['CATEGORY_20'] = output_df['WEEKLY_BUCKET']
    
    # Set quantity produced
    output_df["BC_QUANTITY_PRODUCED"] = output_df["QUANTITY"]

    output_df.loc[output_df['CATEGORY_20'].isin(['', 'None']) | output_df['CATEGORY_20'].isna(),'CATEGORY_19'] = 'unassigned'
    output_df["CATEGORY_18"] = output_df["CP_REMARKS"]
    #output_df.loc[output_df['CATEGORY_20'] == '', 'CATEGORY_19'] = 'unassigned'
    
    # Define template columns (ONLY columns that exist in database table)
    template_cols = [
        "ROW_ID", "PLAN_ID", "PLAN_NAME", "PLAN_START_TIME", "SIMULATION_ID", 
        "SIMULATION_NAME", "ORDER_NUMBER", "ORDER_LINE_NUMBER", "ELIGIBLE_LINE", 
        "ORDER_TYPE", "ITEM_ID", "ITEM_DESCRIPTION", "ORGANIZATION_CODE", 
        "DEPARTMENT_CODE", "FIXED_LOT_MULTIPLIER", "MINIMUM_ORDER_QUANTITY", 
        "MAXIMUM_ORDER_QUANTITY", "UOM", "CONVERSION_FACTOR", "QUANTITY", 
        "QUNATITY_CASE", "QUNATITY_PALLET", "QUNATITY_LITRE", "ORDER_DUE_DATE", 
        "EARLIEST_START_DATE", "EXPIRY_DATE", "PRIORITY", "ABC_CLASSIFICATION", 
        "CATEGORY_1", "CATEGORY_2", "CATEGORY_3", "CATEGORY_4", "CATEGORY_5", 
        "CATEGORY_6", "CATEGORY_7", "CATEGORY_8", "CATEGORY_9", "CATEGORY_10", 
        "CATEGORY_11", "CATEGORY_12", "CATEGORY_13", "CATEGORY_14", "CATEGORY_15", 
        "CATEGORY_16", "CATEGORY_17", "CATEGORY_18", "CATEGORY_19", "CATEGORY_20", 
        "WO_REFERENCE_NUMBER", "WO_RESOURCE_ID", "WO_START_TIME", "WO_END_TIME", 
        "RESOURCE_CODE", "RUN_SPEED", "TOTAL_HOURS_FOR_QTY", "TOTAL_DAYS_FOR_QTY", 
        "BLOCK_CODE", "BC_TASK_TYPE", "BC_START_TIME", "BC_END_TIME", "BC_SEQUENCE", 
        "BC_QUANTITY_PRODUCED", "BC_QUANTITY_PRODUCED_CASE", "BC_QUANTITY_PRODUCED_PALLET", 
        "BC_QUANTITY_PRODUCED_LITRE", "FINAL_TOTAL_HOURS_FOR_QTY", "FINAL_TOTAL_DAYS_FOR_QTY", 
        "DATA_SUFFICIENT", "SCHEULED_IN_PLANNING_HORIZON", "PALLETS_QUANTITY", 
        "LITRE_QUANTITY", "PALLETS_QUANTITY_PRODUCED", "LITRE_QUANTITY_PRODUCED"
    ]
    
    # Create final DataFrame with only template columns
    final_df = pd.DataFrame({
        col: output_df[col] if col in output_df.columns else pd.NA 
        for col in template_cols
    })
    
    # Format ITEM_ID with leading zeros (7 digits)
    if 'ITEM_ID' in final_df.columns:
        final_df['ITEM_ID'] = final_df['ITEM_ID'].apply(
            lambda x: str(x).zfill(7) if pd.notna(x) and str(x) != 'None' and str(x) != 'nan' else None
        )
    
    logger.info(f"Output finalization complete: {len(final_df)} rows, {len(final_df.columns)} columns")
    
    # Save to CSV with proper date format
    csv_filename = "final_df_db_input.csv"
    final_df.to_csv(csv_filename, index=False, date_format="%d-%m-%Y %H:%M:%S")
    logger.info(f"Saved finalized data to {csv_filename}")
    
    return final_df


# ============================================================================
# MAIN PROCESSING FUNCTION
# ============================================================================

def run_production_planning(simulation_id, output_csv_path=None, insert_to_db=True):
    """
    Main function to run the complete production planning process.
    
    Parameters:
        simulation_id: The simulation ID to process
        output_csv_path: Optional path to save the output CSV file
        insert_to_db: Whether to insert results back to database (default: True)
        
    Returns:
        DataFrame: The final processed output
    """
    conn = None
    tunnel = None
    
    try:
        logger.info(f"="*80)
        logger.info(f"Starting Production Planning for Simulation: {simulation_id}")
        logger.info(f"="*80)
        
        # Step 1: Connect to database
        logger.info("Step 1: Connecting to Oracle database")
        conn, tunnel = get_oracle_connection()
        
        # Update sync status to RUNNING
        insert_sync_status(conn=conn, simulation_id=simulation_id, status='RUNNING', error_msg=None)
        
        # Step 2: Fetch data
        logger.info("Step 2: Fetching simulation data")
        data = fetch_simulation_data(conn, simulation_id)
        
        df_resource_availability = data['df_resource_availability']
        df_downtime = data['df_downtime']
        df_CIP_DOWNTIME = data['df_CIP_DOWNTIME']
        df_run_rules = data['df_run_rules']
        df_input = data['df_input']

        df_input["PLAN_START_TIME"] = pd.Timestamp.now()
        df_resource_availability["PLAN_START_TIME"] = pd.Timestamp.now()
        # Step 2: Filter to only records flagged sufficient
        df_input = df_input[df_input["DATA_SUFFICIENT"] == "Y"]

        df_change_over_matrix = data['df_change_over_matrix']
        
        # Step 3: Calculate weekly resource availability
        logger.info("Step 3: Calculating weekly resource availability")
        df_weekly_availability = calculate_resource_availability(
            df_resource_availability, 
            df_downtime, 
            df_CIP_DOWNTIME
        )
        
        # Retrieve the intermediate dataframes we need later
        df_weekly_unavailability = df_weekly_availability._weekly_unavailability
        df_downtime_split = df_weekly_availability._downtime_split
        df_cip_processed = df_weekly_availability._cip_downtime
        
        # Step 4: Add week numbers to run rules
        logger.info("Step 4: Adding week numbers to run rules")
        df_run_rules = add_week_num(df_run_rules, date_column='START_DATETIME', week_column_name='WEEK_NUM')
        
        # Step 5: Add week numbers to input orders
        logger.info("Step 5: Adding week numbers to input orders")
        df_input = add_week_num(df_input, date_column='ORDER_DUE_DATE', week_column_name='WEEK_NUM')
        
        # Step 6: Add run rule flag
        logger.info("Step 6: Adding run rule flags")
        df_input = add_run_rule_flag(df_input, df_run_rules)
        
        # Step 7: Add expiry flag
        logger.info("Step 7: Adding expiry flags")
        df_input = add_expiry_flag(df_input)
        
        # Step 8: Sort input by priority
        logger.info("Step 8: Sorting input by priority")
        output_df = sort_input_by_priority(df_input)
        
        # Step 9: Assign weekly buckets
        logger.info("Step 9: Assigning weekly buckets")
        output_df, df_resource_availability_updated = assign_weekly_bucket(
            output_df, 
            df_run_rules, 
            df_weekly_availability, 
            df_change_over_matrix,
            resource_unavailibility_df=None, 
            normal_downtime_df=None
        )
        
        # Step 10: Calculate BC times
        logger.info("Step 10: Calculating BC times")
        output_df = calculate_bc_times(output_df)
        
        # Step 11: Add changeover tasks
        logger.info("Step 11: Adding changeover tasks")
        output_df = add_changeover_task(output_df)
        
        # Step 12: Add CIP downtime
        logger.info("Step 12: Adding CIP downtime")
        output_df = add_CIP_downtime(output_df, df_cip_processed, df_weekly_availability)
        
        # Step 13: Add unavailability rows
        logger.info("Step 13: Adding unavailability rows")
        output_df = add_unavailability_from_weekly_df(output_df, df_weekly_unavailability)
        
        # Step 14: Add downtime rows
        logger.info("Step 14: Adding downtime rows")
        output_df = add_downtime_from_split(output_df, df_downtime_split)
        
        # Step 15: Finalize output
        logger.info("Step 15: Finalizing output")
        output_df = finalize_output(output_df)
        
        # Step 16: Save output to CSV
        if output_csv_path:
            logger.info(f"Step 16: Saving output to {output_csv_path}")
            mask = (
                output_df["CATEGORY_19"].astype(str).str.lower().eq("unassigned") &
                output_df["BC_TASK_TYPE"].isna()
            )
            output_df.loc[mask, "BC_TASK_TYPE"] = "TASK"
            output_df.to_csv(output_csv_path, index=False, date_format="%d-%m-%Y %H:%M:%S")
            logger.info(f"Output saved successfully: {len(output_df)} rows")
        
        # Step 17: Insert to database
        if insert_to_db and conn:
            logger.info("Step 17: Inserting data to database")
            insert_data_to_db(output_df, conn, simulation_id)
            
            # Update sync status to COMPLETED
            insert_sync_status(conn=conn, simulation_id=simulation_id, status='COMPLETED', error_msg=None)
        
        logger.info(f"="*80)
        logger.info(f"Production Planning Complete!")
        logger.info(f"Total output rows: {len(output_df)}")
        logger.info(f"  - TASK rows: {(output_df['BC_TASK_TYPE'] == 'TASK').sum()}")
        logger.info(f"  - CHANGEOVER rows: {(output_df['BC_TASK_TYPE'] == 'CHANGEOVER').sum()}")
        logger.info(f"  - CIP_DOWNTIME rows: {(output_df['BC_TASK_TYPE'] == 'CIP_DOWNTIME').sum()}")
        logger.info(f"  - RESOURCE_UNAVAILABLE rows: {(output_df['BC_TASK_TYPE'] == 'RESOURCE_UNAVAILABLE').sum()}")
        logger.info(f"  - DOWNTIME rows: {(output_df['BC_TASK_TYPE'] == 'DOWNTIME').sum()}")
        logger.info(f"="*80)
        
        return output_df
        
    except Exception as e:
        logger.error(f"Error during production planning: {e}", exc_info=True)
        
        # Update sync status to FAILED
        if conn:
            try:
                insert_sync_status(conn=conn, simulation_id=simulation_id, status='FAILED', error_msg=str(e))
            except:
                pass
        
        raise
        
    finally:
        # Close connections
        if conn:
            try:
                conn.close()
                logger.info("Oracle connection closed")
            except Exception as e:
                logger.error(f"Error closing Oracle connection: {e}")
        
        if tunnel:
            try:
                tunnel.stop()
                logger.info("SSH tunnel closed")
            except Exception as e:
                logger.error(f"Error closing SSH tunnel: {e}")


# ============================================================================
# FASTAPI BACKGROUND TASK WRAPPER
# ============================================================================

async def api_main(simulation_id):
    """
    Background task wrapper for FastAPI endpoint.
    Runs the production planning process asynchronously.
    """
    try:
        logger.info(f"Background task started for simulation: {simulation_id}")
        
        # Run the production planning process
        output_csv_path = f"AG_PP_OPTIMIZED_OUTPUT_{simulation_id}.csv"
        result_df = run_production_planning(
            simulation_id=simulation_id,
            output_csv_path=output_csv_path,
            insert_to_db=True
        )
        
        logger.info(f"Background task completed successfully for simulation: {simulation_id}")
        return result_df
        
    except Exception as e:
        logger.error(f"Background task failed for simulation {simulation_id}: {e}", exc_info=True)
        raise


# ============================================================================
# FASTAPI ENDPOINTS
# ============================================================================

@app.post("/agb_wheel_group_23")
async def run_ag_pp_engine(request: Request, background_tasks: BackgroundTasks):
    try:
        logger.info(str("Request Receieved"))
        input_data = await request.json()
        sim_id = input_data.get('SIMULATION_ID')
       
        if not sim_id:
            raise HTTPException(status_code=400, detail="Missing SIMULATION_ID")

        background_tasks.add_task(api_main, sim_id)
        return {"status": "started", "message": f"Python Engine task started for instance {sim_id}"}
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "message": str(e)})


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("ag_pp_ps1_2101:app", host="0.0.0.0", port=5001, reload=True)
