import datetime
import pickle

import oracledb
from oracledb import exceptions

from utils import constants

# dsn = oracledb.makedsn(host='oracle.cise.ufl.edu', port=1521, service_name='orcl')
# conn = oracledb.connect(user=constants.DB_DETAILS['USER'], password=constants.DB_DETAILS['PASSWORD'], dsn=dsn)

db_insert_executed = {}

try:
    with open('../status_dump/db_insert.pickle', 'rb+') as handle:
        db_insert_executed = pickle.load(handle)
except FileNotFoundError:
    print("DB INSERT DUMP FILE NOT FOUND")

sql_files = ['../sql_generated/yfin_stock.sql']

# db_insert_executed[sql_files[0]] = 198000

connection_recycle_nature = 1000


def insert():
    dsn = oracledb.makedsn(host='oracle.cise.ufl.edu', port=1521, service_name='orcl')
    conn = oracledb.connect(user=constants.DB_DETAILS['USER'], password=constants.DB_DETAILS['PASSWORD'], dsn=dsn)
    execution_count = 0
    cursor = conn.cursor()
    commit_time = datetime.datetime.now()

    for sql_file in sql_files:
        with open(sql_file, 'r+') as fp:

            statements = fp.readlines()

            print(f"File [{sql_file}] is read")

            file_exec_count = 0

            for statement in statements:

                if file_exec_count < db_insert_executed.get(sql_file, 0):
                    execution_count += 1
                    file_exec_count += 1

                    if file_exec_count % connection_recycle_nature == 0:
                        print(f"Skipped [ {file_exec_count} ] inserts as they were already inserted")

                    continue

                cursor.execute(statement[:-2])

                file_exec_count += 1
                execution_count += 1

                if execution_count % connection_recycle_nature == 0:
                    conn.commit()

                    conn.close()

                    conn = oracledb.connect(user=constants.DB_DETAILS['USER'],
                                            password=constants.DB_DETAILS['PASSWORD'],
                                            dsn=dsn)

                    cursor = conn.cursor()

                    db_insert_executed[sql_file] = file_exec_count

                    with open('../status_dump/db_insert.pickle', 'wb+') as handle:
                        pickle.dump(db_insert_executed, handle)

                    print(
                        f"Inserted [ {connection_recycle_nature} ] statements executed totalling [ {execution_count} ] in [{(datetime.datetime.now() - commit_time)}]", )

                    commit_time = datetime.datetime.now()

            conn.commit()
            conn.close()

            conn = oracledb.connect(user=constants.DB_DETAILS['USER'], password=constants.DB_DETAILS['PASSWORD'],
                                    dsn=dsn)

            cursor = conn.cursor()

            db_insert_executed[sql_file] = file_exec_count

            with open('../status_dump/db_insert.pickle', 'wb+') as handle:
                pickle.dump(db_insert_executed, handle)


while True:
    try:
        insert()
        break
    except exceptions.DatabaseError as e:
        print(e)
        print("Connection Closed on the DB side. Restarting........")

print(f"All rows inserted successfully")
