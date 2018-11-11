import sqlite3
import socket, struct
import json
from flask import Flask,request,g,jsonify
from OpenSSL import SSL
from contextlib import closing


DATABASE='/tmp/api.db'

def ip2long(ip):
	packedIP = socket.inet_aton(ip)
	return struct.unpack("!L", packedIP)[0]

def checkTableName(tablename):
	if not 'BA' in tablename:
		if not 'CA' in tablename:
			return False
	return True
	
def getInvalidTableError():
	return "{'error': {'code': -32502,'message':'Invalid Table'}}"

def getServiceTypeError():
	return "{'error': {'code': -32501,'message':'Service Type not found'}}"

def getInvalidParametersError(id):
	return "[{\"error\": {\"code\": -32602,\"message\": \"Invalid method parameter(s).\"},\"id\": %s, \"jsonrpc\": \"2.0\" }]"%(id) 

def getConflictingEntryError(conflict):
	return "{'error': {'code': -32503,'message':'Conflicting Table Entry : %s'}}"%(str(conflict))
	
def initDb():
	with closing(connectDb()) as db:
		with app.open_resource('schema.sql',mode='r') as f:
			db.cursor().executescript(f.read())
			db.commit()

def connectDb():
	return sqlite3.connect(app.config['DATABASE'])

def dispatchDeactivationBA(params,id):
	global retVal
	global iserror
	iserror=False
	

	if not params['ClientIP']:
		retVal = getInvalidParametersError(id)
		iserror=True
		return str(retVal),iserror


	clientip= params['ClientIP']

	retVal = "{'ClientIP': '%s'}"%(clientip) 
	sqlstr="delete from activations where clientip='%s'"%(clientip)
	try:
		cur = g.db.execute(sqlstr)
		g.db.commit()
	except:
		print("Exception")	
	return str(retVal),iserror 

def dispatchActivationBA(params,id):
	global retVal
	global iserror
	iserror=False
	
	if not params['DurationSec']:
		retVal = getInvalidParametersError(id)
		iserror=True
		return str(retVal),iserror

	if not params['ClientIP']:
		retVal = getInvalidParametersError(id)
		iserror=True
		return str(retVal),iserror

	if not params['ServiceType']:
		retVal = getInvalidParametersError(id)
		iserror=True
		return str(retVal),iserror

	servicetype = params['ServiceType']
	clientip= params['ClientIP']
	duration= params['DurationSec']

	global emptyRow
	emptyRow = True
	g.db = connectDb()
	sqlstr = "select servicetype,policyname from servicetypes where servicetype='%s'"%(servicetype)
	try:
		cur = g.db.execute(sqlstr)
		for row in cur.fetchall():
			foundServiceType = row[0]
			foundPolicyName = row[1]
			emptyRow = False
			break
	except Exception.e:
		print(str(e)) 

	if emptyRow:
		retVal = getServiceTypeError()
		return str(retVal), iserror 
	else:
		retVal = "{'ClientIP': '%s', 'ServiceType': '%s', 'DurationSec': %s}"%(clientip,servicetype,duration) 
		cur = g.db.execute('insert into activations(clientip,servicetype,duration) VALUES(?,?,?)',(clientip,servicetype,duration))
		g.db.commit()	
	return str(retVal),iserror 



def dispatchDeleteServiceType(params,id):
	global retVal
	global iserror
	iserror=False
	
	if not params['TableName']:
		retVal = getInvalidParametersError(id)
		iserror=True
		return str(retVal), iserror

	if not params['ServiceType']:
		retVal = getInvalidParametersError(id)
		iserror=True
		return str(retVal),iserror

	if not checkTableName(params['TableName']):
		retVal = getInvalidTableError() 
		return str(retVal),iserror

	servicetype = params['ServiceType']

	global emptyRow
	emptyRow = True
	g.db = connectDb()
	sqlstr = "select servicetype,policyname from servicetypes where servicetype='%s'"%(servicetype)
	try:
		cur = g.db.execute(sqlstr)
		for row in cur.fetchall():
			foundServiceType = row[0]
			foundPolicyName = row[1]
			emptyRow = False
			break
	except Exception.e:
		print(str(e)) 

	if emptyRow:
		retVal = "{'policies': []"
	else:
		sqlstr="delete from servicetypes where servicetype='%s' and policyname='%s'"%(foundServiceType,foundPolicyName)
		cur = g.db.execute(sqlstr)
		g.db.commit()	
		retVal = "{'policies': [{'ServiceType':'%s','PolicyName':'%s'}]"%(foundServiceType,foundPolicyName) 
	return str(retVal),iserror 

def dispatchUpdateServiceType(params,id):
	global retVal
	global iserror
	iserror=False
	
	if not params['TableName']:
		retVal = getInvalidParametersError(id)
		iserror=True
		return str(retVal), iserror

	if not params['ServiceType']:
		retVal = getInvalidParametersError(id)
		iserror=True
		return str(retVal),iserror

	if not params['PolicyName']:
		retVal = getInvalidParametersError(id)
		iserror=True
		return str(retVal),iserror

	if not checkTableName(params['TableName']):
		retVal = getInvalidTableError() 
		return str(retVal),iserror

	servicetype = params['ServiceType']
	policyname= params['PolicyName']

	global emptyRow
	emptyRow = True
	g.db = connectDb()
	sqlstr = "select servicetype,policyname from servicetypes where servicetype='%s'"%(servicetype)
	try:
		cur = g.db.execute(sqlstr)
		for row in cur.fetchall():
			foundServiceType = row[0]
			foundPolicyName = row[1]
			emptyRow = False
			break
	except Exception.e:
		print(str(e)) 

	if emptyRow:
		retVal = "{'policies': []"
	else:
		sqlstr="update servicetypes set policyname='%s' where servicetype='%s' and policyname='%s'"%(policyname,foundServiceType,foundPolicyName)
		cur = g.db.execute(sqlstr)
		g.db.commit()	
		retVal = "{'policies': [{'ServiceType':'%s','PolicyName':'%s'}]"%(servicetype,policyname) 
	return str(retVal),iserror 

def dispatchAddServiceType(params,id):
	global retVal
	global iserror
	iserror=False
	
	if not params['TableName']:
		retVal = getInvalidParametersError(id)
		iserror=True
		return str(retVal), iserror

	if not params['ServiceType']:
		retVal = getInvalidParametersError(id)
		iserror=True
		return str(retVal),iserror

	if not params['PolicyName']:
		retVal = getInvalidParametersError(id)
		iserror=True
		return str(retVal),iserror

	if not checkTableName(params['TableName']):
		retVal = getInvalidTableError() 
		return str(retVal),iserror

	servicetype = params['ServiceType']
	policyname= params['PolicyName']

	# check for conflicting entry
	g.db = connectDb()
	sqlstr = "select servicetype,policyname from servicetypes where servicetype='%s'"%(servicetype)
	try:
		cur = g.db.execute(sqlstr)
		for row in cur.fetchall():
			conflict = "{'ServiceType':'%s','PolicyName':'%s'},"%(row[0],row[1])
			conflict = conflict[:-1]
			retVal= getConflictingEntryError(conflict)
			return retVal,iserror
	except:
		print("select exception")
	
	cur = g.db.execute('insert into servicetypes(servicetype,policyname) VALUES(?,?)',(servicetype,policyname))
	g.db.commit()	
	retVal = "{'policies': [{'ServiceType':'%s','PolicyName':'%s'}]"%(servicetype,policyname) 
	return str(retVal),iserror 

	
def dispatchReadAllServiceTypes(params,id):
	global retVal
	global iserror
	iserror=False

	if not params['TableName']:
		retVal = getInvalidParametersError(id)
		iserror = True
		return str(retVal),iserror

	if not checkTableName(params['TableName']):
		retVal = getInvalidTableError() 
		return str(retVal),iserror

	# Not an error ,,get tables entries
	retVal = "{'policies': [ " 
	g.db = connectDb()
	cur = g.db.execute('select servicetype, policyname from servicetypes')
	for row in cur.fetchall():
		retVal = retVal + "{'ServiceType':'%s','PolicyName':'%s'},"%(row[0],row[1])
	retVal = retVal[:-1]
	retVal = retVal + "]}"
	return str(retVal), iserror 

def dispatchReadAllRules(params,id):
	global retVal
	global iserror
	iserror=False

	if not params['TableName']:
		retVal = getInvalidParametersError(id)
		iserror = True
		return str(retVal),iserror

	if not checkTableName(params['TableName']):
		retVal = getInvalidTableError() 
		return str(retVal),iserror

	# Not an error ,,get tables entries
	retVal = "{'policies': [ " 
	g.db = connectDb()
	cur = g.db.execute('select serialnumber, servicetype, startip, endip from rules')
	for row in cur.fetchall():
		retVal = retVal + "{'SerialNumber':'%s','ServiceType':'%s','StartIP':'%s','EndIP':'%s'},"%(row[0],row[1],row[2],row[3])
	retVal = retVal[:-1]
	retVal = retVal + "]}"
	return str(retVal), iserror 

def dispatchAddRuleEntry(params,id):
	global retVal
	global iserror
	iserror=False
	
	if not params['TableName']:
		retVal = getInvalidParametersError(id)
		iserror=True
		return str(retVal), iserror

	if not params['SerialNumber']:
		retVal = getInvalidParametersError(id)
		iserror=True
		return str(retVal),iserror

	if not params['ServiceType']:
		retVal = getInvalidParametersError(id)
		iserror=True
		return str(retVal),iserror

	if not params['StartIP']:
		retVal = getInvalidParametersError(id)
		iserror=True
		return str(retVal),iserror

	if not params['EndIP']:
		retVal = getInvalidParametersError(id)
		iserror=True
		return str(retVal),iserror

	if not checkTableName(params['TableName']):
		retVal = getInvalidTableError() 
		return str(retVal),iserror

	serialnumber = params['SerialNumber']
	servicetype = params['ServiceType']
	startip= params['StartIP']
	endip= params['EndIP']

	# check for Service Type not found 
	g.db = connectDb()
	sqlstr = "select servicetype from servicetypes where servicetype='%s'"%(servicetype)
	try:
		cur = g.db.execute(sqlstr)
		if not len(list(cur)):
			retVal= getServiceTypeError() 
			return retVal,iserror
	except:
		print("select exception")


    # check for Conflicting entry
	sqlstr = "select serialnumber,servicetype,startip,endip from rules where startip='%s' and endip='%s'"%(startip,endip)
	try:
		cur = g.db.execute(sqlstr)
		for row in cur.fetchall():
			conflict = "{'SerialNumber':'%s','ServiceType':'%s','StartIP':'%s','EndIP','%s'},"%(row[0],row[1],row[2],row[3])
			conflict = conflict[:-1]
			retVal= getConflictingEntryError(conflict)
			return retVal,iserror
	except:
		print("select exception")
	
	cur = g.db.execute('insert into rules(serialnumber,servicetype,startip,endip) VALUES(?,?,?,?)',(serialnumber,servicetype,startip,endip))
	g.db.commit()	
	retVal = "{'policies': [{'SerialNumber':'%s','ServiceType':'%s','StartIP':'%s','EndIP','%s'}]"%(serialnumber,servicetype,startip,endip) 
	return str(retVal),iserror 

def dispatchUpdateRuleEntry(params,id):
	global retVal
	global iserror
	iserror=False
	
	if not params['TableName']:
		retVal = getInvalidParametersError(id)
		iserror=True
		return str(retVal), iserror

	if not params['SerialNumber']:
		retVal = getInvalidParametersError(id)
		iserror=True
		return str(retVal),iserror

	if not params['ServiceType']:
		retVal = getInvalidParametersError(id)
		iserror=True
		return str(retVal),iserror

	if not params['StartIP']:
		retVal = getInvalidParametersError(id)
		iserror=True
		return str(retVal),iserror

	if not params['EndIP']:
		retVal = getInvalidParametersError(id)
		iserror=True
		return str(retVal),iserror

	if not checkTableName(params['TableName']):
		retVal = getInvalidTableError() 
		return str(retVal),iserror

	serialnumber = params['SerialNumber']
	servicetype = params['ServiceType']
	startip= params['StartIP']
	endip= params['EndIP']

	# check for Service Type not found 
	g.db = connectDb()
	sqlstr = "select servicetype from servicetypes where servicetype='%s'"%(servicetype)
	try:
		cur = g.db.execute(sqlstr)
		if not len(list(cur)):
			retVal= getServiceTypeError() 
			return retVal,iserror
	except:
		print("select exception")


    # check for no entry
	global emptyRow
	emptyRow = True
	sqlstr = "select serialnumber,startip,endip from rules where serialnumber='%s' and startip='%s' and endip='%s'"%(serialnumber,startip,endip)
	try:
		cur = g.db.execute(sqlstr)
		for row in cur.fetchall():
			foundserialnumber = row[0]
			foundstartip = row[1]
			foundendip = row[2]
			emptyRow = False
	except:
		print("select exception")
	
	if emptyRow:
		retVal = "{'policies': []"
	else:
		sqlstr="update rules set servicetype='%s' where serialnumber='%s' and startip='%s' and endip='%s'"%(servicetype,foundserialnumber,foundstartip,foundendip)
		cur = g.db.execute(sqlstr)
		g.db.commit()	
		retVal = "{'policies': [{'SerialNumber':'%s','ServiceType':'%s','StartIP':'%s','EndIP','%s'}]"%(serialnumber,servicetype,startip,endip) 
	return str(retVal),iserror 

def dispatchDeleteRuleEntry(params,id):
	global retVal
	global iserror
	iserror=False
	
	if not params['TableName']:
		retVal = getInvalidParametersError(id)
		iserror=True
		return str(retVal), iserror

	if not params['SerialNumber']:
		retVal = getInvalidParametersError(id)
		iserror=True
		return str(retVal),iserror

	if not params['StartIP']:
		retVal = getInvalidParametersError(id)
		iserror=True
		return str(retVal),iserror

	if not params['EndIP']:
		retVal = getInvalidParametersError(id)
		iserror=True
		return str(retVal),iserror

	if not checkTableName(params['TableName']):
		retVal = getInvalidTableError() 
		return str(retVal),iserror

	serialnumber= params['SerialNumber']
	startip = params['StartIP']
	endip = params['EndIP']

	g.db = connectDb()
	sqlstr="delete from rules where serialnumber='%s' and startip='%s' and endip='%s'"%(serialnumber,startip,endip)
	cur = g.db.execute(sqlstr)
	g.db.commit()	
	retVal = "{'policies': [{'SerialNumber':'%s','StartIP':'%s','EndIP':'%s'}]"%(serialnumber,startip,endip) 
	return str(retVal),iserror 



def dispatchFindSNInRuleTable(params,id):

	global retVal
	global iserror
	global serialnumber,startip,endip
	global wildserialnumber,wildstartip,wildendip
	wildserialnumber=False
	wildstartip=False
	wildendip=False
	iserror=False
	
	if not params['TableName']:
		retVal = getInvalidParametersError(id)
		iserror=True
		return str(retVal), iserror

	if not params['SerialNumber']:
		retVal = getInvalidParametersError(id)
		iserror=True
		return str(retVal),iserror

	if not params['StartIP']:
		retVal = getInvalidParametersError(id)
		iserror=True
		return str(retVal),iserror

	if not params['EndIP']:
		retVal = getInvalidParametersError(id)
		iserror=True
		return str(retVal),iserror

	if not checkTableName(params['TableName']):
		retVal = getInvalidTableError() 
		return str(retVal),iserror

	serialnumber = params['SerialNumber']
	startip= params['StartIP']
	endip= params['EndIP']

	# Need to check if there are any wildcards in the fields

	if '*' in serialnumber:
		if len(serialnumber) > 1:
			wildserialnumber = True
		else:
			# invalid parameter ,, * not allowed by itself
			retVal = getInvalidParametersError(id)
			iserror=True
			return str(retVal),iserror
	if startip == '*':
		wildstartip= True
	if endip == '*':
		wildendip= True

	global emptyRow,sqlstr
	emptyRow = True
	# if no wildcards do completely qualified query
	if not wildserialnumber and not wildstartip and not wildendip:
		g.db = connectDb()
		sqlstr = "select serialnumber,servicetype,startip,endip from rules where serialnumber='%s' and startip='%s' and endip='%s'"%(serialnumber,startip,endip)
		try:
			cur = g.db.execute(sqlstr)
			for row in cur.fetchall():
				foundserialnumber=row[0]
				foundservicetype=row[1]
				foundstartip=row[2]
				foundendip=row[3]
				emptyRow = False
		except:
			print("select exception")

		if emptyRow:
			retVal = "{'policies': []"
		else:
			retVal = "{'policies': [{'SerialNumber':'%s','ServiceType':'%s','StartIP':'%s','EndIP':'%s'}]"%(foundserialnumber,foundservicetype,foundstartip,foundendip) 
		return str(retVal),iserror 


	sqlstr = "select serialnumber,servicetype,startip,endip from rules where serialnumber"
	if wildserialnumber:
		# add % to end of serialnumber
		serialnumber = serialnumber[:-1] + "%"
		sqlstr = sqlstr + " like '%s' and startip"%(serialnumber)
	else:
		sqlstr = sqlstr + "= '%s' and startip"%(serialnumber)
	if wildstartip:
		startip ="%"
		sqlstr = sqlstr + " like '%s' and endip"%(startip)
	else:
		sqlstr = sqlstr + "= '%s' and endip"%(startip)
	if wildendip:
		endip = "%" 
		qlstr = sqlstr + " like '%s'"%(endip)
	else:
		sqlstr = sqlstr + "= '%s'"%(endip)

	retVal = "{'policies': [ " 
	g.db = connectDb()
	cur = g.db.execute(sqlstr)
	for row in cur.fetchall():
		retVal = retVal + "{'SerialNumber':'%s','ServiceType':'%s','StartIP':'%s','EndIP':'%s'},"%(row[0],row[1],row[2],row[3])
	retVal = retVal[:-1]
	retVal = retVal + "]}"
	return str(retVal), iserror 

def dispatchFindIPInRuleTable(params,id):

	global retVal
	global iserror
	global serialnumber,startip,endip
	global wildserialnumber,wildstartip,wildendip
	wildserialnumber=False
	wildstartip=False
	wildendip=False
	iserror=False
	
	if not params['TableName']:
		retVal = getInvalidParametersError(id)
		iserror=True
		return str(retVal), iserror

	if not params['ClientIP']:
		retVal = getInvalidParametersError(id)
		iserror=True
		return str(retVal),iserror

	if not checkTableName(params['TableName']):
		retVal = getInvalidTableError() 
		return str(retVal),iserror

	clientip= params['ClientIP']

	global emptyRow,sqlstr
	emptyRow = True

	# get all rows and return 1st hit where client falls in the range
	sqlstr = "select serialnumber,servicetype,startip,endip from rules"
	retVal = "{'policies': [" 
	g.db = connectDb()
	cur = g.db.execute(sqlstr)
	for row in cur.fetchall():
		if (ip2long(clientip) >= ip2long(row[2])) and (ip2long(clientip) <= ip2long(row[3])):  
			retVal = retVal + "{'SerialNumber':'%s','ServiceType':'%s','ClientIP':'%s'}"%(row[0],row[1],clientip)
			break

	retVal = retVal + "]}"
	return str(retVal), iserror 

def dispatchDeleteRuleTable(params,id):

	global retVal
	global iserror
	global serialnumber,startip,endip
	global wildserialnumber,wildstartip,wildendip
	wildserialnumber=False
	wildstartip=False
	wildendip=False
	iserror=False
	
	if not params['TableName']:
		retVal = getInvalidParametersError(id)
		iserror=True
		return str(retVal), iserror

	if not checkTableName(params['TableName']):
		retVal = getInvalidTableError() 
		return str(retVal),iserror

	# delete the table
	sqlstr = "delete from rules"
	g.db = connectDb()
	try:
		cur = g.db.execute(sqlstr)
	except:
		print("Exception deleting table")

	g.db.commit()

	retVal = "{'policies': []}" 

	return str(retVal), iserror 
	


def dispatchDeleteServiceTypeTable(params,id):

	global retVal
	global iserror
	global serialnumber,startip,endip
	global wildserialnumber,wildstartip,wildendip
	wildserialnumber=False
	wildstartip=False
	wildendip=False
	iserror=False
	
	if not params['TableName']:
		retVal = getInvalidParametersError(id)
		iserror=True
		return str(retVal), iserror

	if not checkTableName(params['TableName']):
		retVal = getInvalidTableError() 
		return str(retVal),iserror

	# delete the table
	sqlstr = "delete from servicetypes"
	g.db = connectDb()
	try:
		cur = g.db.execute(sqlstr)
	except:
		print("Exception deleting table")

	g.db.commit()

	retVal = "{'policies': []}" 

	return str(retVal), iserror 


def buildResponse(id, retVal,iserror):
	# if iserror treat as system error 
	if iserror:
		return retVal
	# otherwise treat as application error
	resp="[{\"jsonrpc\":\"2.0\",\"id\":%s,\"result\":{\"data\":\"%s\"}}]"%(id,retVal)
	return resp


app = Flask(__name__)
app.config.from_object(__name__)

@app.route('/policyengine/v1/functions', methods=['POST'])
def parse_jsonrpc():
	# TODO : come back and do basic checks
	content = request.get_json(force=True)
	rpcmethod = content['method']
	params = content['params']
	id = content['id']
	if 'ReadAllServiceTypes' in rpcmethod:
		retval,iserror = dispatchReadAllServiceTypes(params,id)
	if 'AddServiceType' in rpcmethod:
		retval,iserror = dispatchAddServiceType(params,id)
	# Note ..UpdateServiceTypeForRuleTable must be checked first
	if 'UpdateServiceTypeForRuleTable' in rpcmethod:
		retval,iserror = dispatchUpdateRuleEntry(params,id)
	else:
		if 'UpdateServiceType' in rpcmethod:
			retval,iserror = dispatchUpdateServiceType(params,id)
	if 'DeleteServiceType' in rpcmethod:
		retval,iserror = dispatchDeleteServiceType(params,id)
	if 'ActivationBA' in rpcmethod:
		retval,iserror = dispatchActivationBA(params,id)
	if 'DeactivationBA' in rpcmethod:
		retval,iserror = dispatchDeactivationBA(params,id)
	if 'ReadAllFromRuleTable' in rpcmethod:
		retval,iserror = dispatchReadAllRules(params,id)
	if 'AddEntryToRuleTable' in rpcmethod:
		retval,iserror = dispatchAddRuleEntry(params,id)
	if 'DeleteEntryFromRuleTable' in rpcmethod:
		retval,iserror = dispatchDeleteRuleEntry(params,id)
	if 'QueryBySerialNumberFromRuleTable' in rpcmethod:
		retval,iserror = dispatchFindSNInRuleTable(params,id)
	if 'QueryByIPFromRuleTable' in rpcmethod:
		retval,iserror = dispatchFindIPInRuleTable(params,id)
	if 'DeleteAllEntriesFromRuleTable' in rpcmethod:
		retval,iserror = dispatchDeleteRuleTable(params,id)
	if 'DeleteAllEntriesFromServiceTypeTable' in rpcmethod:
		retval,iserror = dispatchDeleteServiceTypeTable(params,id)
	resp = buildResponse(id,retVal,iserror)
	return resp 

if __name__ == "__main__":
	initDb()
	app.run(host='0.0.0.0',ssl_context='adhoc')


