'''
Created on Apr 20, 2016

@author: andres.fandino@oracle.com
Oracle Consulting NA-TAG
'''
import os
import shutil as util
from datetime import datetime

# Set the inbox
strSrcFolder =  fdmContext["INBOXDIR"]
os.chdir(strSrcFolder)
fdmAPI.logInfo("Looking for files in Inbox: " + strSrcFolder)
# Grab a list of all locations that have an IO1 set
strSQL = "SELECT PARTNAME, PARTINTGCONFIG1 FROM TPOVPARTITION WHERE PARTCONTROLSTYPE = 1 AND PARTINTGCONFIG1 <> ''"
lstParams = []
try:
  rs = fdmAPI.executeQuery(strSQL,lstParams)
except Exception, e:
  print "Database connection Error "+ str(e)
# Create a datetime stamp to put on file
now = datetime.now()
lstDate= [now.year,now.month, now.day, now.hour, now.minute, now.second]
dtNow = ""
for x in lstDate:
  dtNow = dtNow + "." + str(x)
# If any location are found
for f in os.listdir("."):
  pathName, fileName = os.path.split(f)
  if os.path.isfile(fileName):
    while rs.next():
      strListOfFilePatternsPerLocation = rs.getString("PARTINTGCONFIG1")
      aPatterns = strListOfFilePatternsPerLocation.split(";")           
      for a in aPatterns:              
        fName, fExt = os.path.splitext(f)
        fdmAPI.logInfo("InboxFileOrganizer : ++++++++++++++++++              FName : "+fName )    
        if f.upper().startswith(a.upper()):
          fdmAPI.logInfo("InboxFileOrganizer : ++++++++++++++++++          pattern : " + a + "  filename : " + f)
          fdmAPI.logInfo("InboxFileOrganizer : ++++++++++++++++++          LocationName : " + rs.getString("PARTNAME") )
          sourceFile = strSrcFolder + "\\" + f
          destFile = strSrcFolder +"\\"+ rs.getString("PARTNAME") +  "\\" + fName + dtNow + fExt
          util.move(sourceFile, destFile)
          fdmAPI.logInfo("File moved from:" + sourceFile)
          fdmAPI.logInfo("File moved to: " + destFile)
          break
        else:
          fdmAPI.logInfo("No files found for pattern: " + a)
    rs = fdmAPI.executeQuery(strSQL,lstParams)      
  else:
    fdmAPI.logInfo("Skipping directory: " + f)
  
fdmAPI.closeConnection()
