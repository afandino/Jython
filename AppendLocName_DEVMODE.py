'''
Created on Apr 20, 2016

@author: andres.fandino@oracle.com
Oracle Consulting NA-TAG
'''
import os
import shutil as util
from datetime import datetime
import java.sql as sql
import java.math.BigDecimal as BigDecimal



# DEV MODE
import com.hyperion.aif.scripting.API as fdmeeAPI
dbUrl= "jdbc:sqlserver://SERVERNAME\SQLCLUSTER;databaseName=XXXX;"
dbUser="HYP_DB_USER"
dbPass="password"
dbDriver="JDBC.SQLServerDriver"
LoadID = 12
sqlconn = sql.DriverManager.getConnection(dbUrl, dbUser,dbPass)
fdmAPI = fdmeeAPI()
fdmAPI.initializeDevMode(sqlconn)
fdmContext = fdmAPI.initContext(BigDecimal(LoadID))
# END DEV MODE


#Set the outbox
strSrcFolder =  fdmContext["OUTBOXDIR"]
os.chdir(strSrcFolder)
fdmAPI.logInfo("Looking for files in Outbox: " + strSrcFolder)

strSQL="SELECT PROCESS_ID, PARTNAME FROM AIF_PROCESSES PRC JOIN TPOVPARTITION PAR ON PRC.PARTITIONKEY = PAR.PARTITIONKEY WHERE STATUS = 'SUCCESS' AND RULE_TYPE = 'DATA' AND PROCESS_ID="
sqlParams = []
lstParams = {}

# Create a datetime stamp to put on file
now = datetime.now()
lstDate= [now.year,now.month, now.day, now.hour, now.minute, now.second]
dtNow = ""
for x in lstDate:
  dtNow = dtNow + str(x)
# List files on the outbox dir
for f in os.listdir("."):
  pathName, fileName = os.path.split(f)
  if os.path.isfile(fileName):
    sourceFile, fileExt = os.path.splitext(fileName)
    preSep, itsep, aftsep = sourceFile.partition("_")
    if preSep == 'SHFM' and fileExt == '.dat' and aftsep:              
      lstParams[aftsep] = fileName
#      print lstParams     
  else:
    fdmAPI.logInfo("Skipping directory: " + f)
# Getting Location From DB  
for item in lstParams:
  stmQry = strSQL + item
  strsourceFile = os.path.join('.',lstParams[item])
    
  try:
    rs = fdmAPI.executeQuery(stmQry,sqlParams)
    while rs.next():              
      strProcessID = rs.getString("PROCESS_ID")
      strLocation = rs.getString("PARTNAME")
      # dFile = strLocation + '_' + dtNow + '_' + lstParams[item]
      dFile = strLocation + '_' + lstParams[item]
      destFile = os.path.join('.',dFile)
      fdmAPI.logInfo("Query Executed: " + stmQry)
      fdmAPI.logInfo("Process ID found: " + strProcessID)  
      fdmAPI.logInfo("Location found: " + strLocation)
      fdmAPI.logInfo("Original filename: " + strsourceFile)
      fdmAPI.logInfo("New filename: " + destFile)
      if strProcessID and strLocation:
        util.move(strsourceFile, destFile)
  except Exception, e:
    print "Database connection Error "+ str(e)

fdmAPI.closeConnection() 
