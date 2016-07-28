'''
Created on Apr 4, 2016

@author: fand
'''
#-------------------------------------------------------------------------------------------#
# Purpose: Extract data from source table and load open interface table
# November 2015
# Oracle Corporation
#-------------------------------------------------------------------------------------------#
#
#--------------------------------------------------------------------#
# Import Java sql class for data management operations   
#--------------------------------------------------------------------#
import java.sql as sql
import com.hyperion.aif.scripting.API as fdmAPI
#--------------------------------------------------------------------#
# Define variables and set up INSERT statement that will  
# be used to insert into the open interface table                
#--------------------------------------------------------------------#
batchName = "ACTUAL_PD_DATA"
myDataView = "Periodic"
myCurrency = "USD"
myScenario = "Actual"
myVersion = "Final"
myView = "Ledger"
myProject = "All Projects"
myFundSrc = "10R"
myEntity = "484C02021"

insertStmt = """
INSERT INTO AIF_OPEN_INTERFACE (
BATCH_NAME
,DATAVIEW
,CURRENCY
,AMOUNT
,COL01
,COL02
,COL03
,COL04
,COL05
,COL06
,COL07
,COL08
,COL09
,COL14
,COL18
,COL11
,COL12
,COL13
,COL10
,COL15
,COL16
,COL17
) VALUES (
?
,?
,?
,?
,?
,?
,?
,?
,?
,?
,?
,?
,?
,?
,?
,?
,?
,?
,?
,?
,?
,?)
"""

#---------------------------------------------------#
# Connect to source database and schema
#---------------------------------------------------#
sourceConn = sql.DriverManager.getConnection("jdbc:oracle:thin:@db-t1exaepm-tst.tgdot.tst.local:1533:t1exaepm", "fdmee", "fdmee");

selectStmt = """
SELECT POSTED_TOTAL_AMT,
ACCOUNTING_PERIOD,
FUND_CODE,
ACCOUNT,
CLASS_FLD,
DEPTID,
DEPT_DESCR,
PROJECT_ID,
DESCR,
PROGRAM_CODE,
CHARTFIELD1,
FISCAL_YEAR,
LEDGER
FROM GDOTUSER.VW_EDW_BUDGET_INQ_FYAP_DETAILS WHERE FISCAL_YEAR=? AND ACCOUNTING_PERIOD=? AND BUDGET_PERIOD=? AND LEDGER IN(?,?)
"""

stmt = sourceConn.createStatement()
stmtRS = stmt.executeQuery(selectStmt)

#--------------------------------------------------------------------------#
# Loop through source table and write to open interface table
#--------------------------------------------------------------------------#
while(stmtRS.next()):
  params = [ batchName, 
  myDataView,
  myCurrency,
  stmtRS.getBigDecimal("POSTED_TOTAL_AMT"),
  stmtRS.getString("ACCOUNTING_PERIOD"),
  myScenario,
  stmtRS.getString("FUND_CODE"),
  stmtRS.getString("ACCOUNT"),
  stmtRS.getString("CLASS_FLD"),
  stmtRS.getString("DEPTID"),
  stmtRS.getString("DEPT_DESCR"),
  stmtRS.getString("PROJECT_ID"),
  stmtRS.getString("DESCR"),
  myEntity,
  stmtRS.getString("PROGRAM_CODE"),
  stmtRS.getString("CHARTFIELD1"),
  stmtRS.getString("FISCAL_YEAR"),
  stmtRS.getString("LEDGER"),
  myFundSrc,
  myVersion,
  myView,
  myProject]
  fdmAPI.executeDML(insertStmt, params, False)
  fdmAPI.commitTransaction()
#---------------------------------------------#
# close connections
#---------------------------------------------#
stmtRS.close()
stmt.close()
sourceConn.close()
