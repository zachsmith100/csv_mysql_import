import sys
import string
import csv
import logging
import json
import subprocess

################################################################################
# Initialize
################################################################################
logging.basicConfig(level=logging.DEBUG)

if len(sys.argv) < 2:
    logging.error("Expected json config filename argument missing.")
    sys.exit(-1)

################################################################################
# Load config
################################################################################
configFilename = sys.argv[1]

with open(configFilename) as configFile:
    config = json.load(configFile)

connectionProfile = config["connectionProfile"]
tableSchema = config["tableSchema"]

################################################################################
# Write command file
################################################################################
with open("csv_import.sql", "w") as outputFile:

    # Create DB
    ###########
    outputFile.write("CREATE DATABASE IF NOT EXISTS ")
    outputFile.write(connectionProfile['dbName'])
    outputFile.write(" DEFAULT CHARACTER SET 'utf8';")
    outputFile.write("\n\n")

    # Use DB
    ########
    sql = "USE {0};".format(connectionProfile["dbName"])
    outputFile.write(sql)
    outputFile.write("\n\n")

    # Drop Table
    ############
    sql = "DROP TABLE IF EXISTS {0};".format(connectionProfile['tableName'])
    outputFile.write(sql)
    outputFile.write("\n\n")

    # Create Table
    ##############
    fieldNames = tableSchema["fields"].split(',')
    fields = []

    for fieldName in fieldNames:
        fields.append("{0} {1}\n".format(
            fieldName,
            tableSchema["fieldDefinitions"][fieldName]["type"]))

    fields.append("id int not null AUTO_INCREMENT\n")
    fields.append("Primary KEY (id)\n")

    sql = "CREATE TABLE {0} (\n{1});".format(
        connectionProfile["tableName"],
        string.join(fields, ','))

    outputFile.write(sql)
    outputFile.write("\n\n")

    # Import CSV
    ############
    outputFile.write("LOAD DATA LOCAL INFILE '{0}' ".format(
        config["importCsvFilename"]))
    outputFile.write("INTO TABLE {0} FIELDS TERMINATED BY ',' ".format(
        connectionProfile["tableName"]))
    outputFile.write("ENCLOSED BY '\"' ")
    outputFile.write("LINES TERMINATED BY '\\n' ")
    outputFile.write("IGNORE 1 ROWS;")
    outputFile.write("\n\n")

################################################################################
# Execute Queries
################################################################################
cmd = "mysql --user={0} --password={1} < csv_import.sql".format(
    connectionProfile["user"],
    connectionProfile["password"])

returnValue = subprocess.call(cmd, shell=True)

logging.info("Script result: {0}".format(returnValue))

sys.exit(returnValue)
