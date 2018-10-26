import json
import datetime as dt

from  pandas import DataFrame
import pandas as pd
import requests

from settings import Config
def SOQL(Instance, SOQL):
    """Executes SOQL using passed Session.
    Parameters
    __________
    Instance: Instance object
    Valid salesforce instance provided by requests.session object
    passed into simple-salesforce
    e.g.
    session=requests.Session()
    sf = Salesforce(username=user, password=pw, security_token=key, session=session)

    SOQL: Query as string
    Valid SOQL Query

    Returns: Dataframe tuple
    A Tuple of (pandas_dataframe, record_count)

    pandas_dataframe is a dataframe extracted from 1 level of returned values.
    References in passed SOQL will be represented as a column of OrderedDicts
    Returns None when the query produces no result

    record_count is the value returned from SFDC. If pandas_dateframe.shape[0]
    is not equal to record_count the dataframe is likely invalid
    _______


    """
    qryResult = Instance.query(SOQL)
    recordCount = qryResult['totalSize']
    isDone = qryResult['done']

    #if isDone == True:
    df = DataFrame(qryResult['records'])
    counter=0
    #print('Before While', df.shape)
    while isDone != True:
        try:
            if qryResult['done'] != True:
                #print ("Before df population, Count:", counter)
                qryResult = Instance.query_more(qryResult['nextRecordsUrl'], True)
                df = df.append(DataFrame(qryResult['records']))
                #print ("After df population, Count:", counter)
            else:
                #df = df.append(DataFrame(qryResult['records']))
                isDone = True
                #print('completed')
                break
            counter = counter + 1
        except NameError:
            print("NameError on loop ", counter)
            #df = DataFrame(qryResult['records'])
            #qry = Instance.query_more(qryResult['nextRecordsUrl'], True)

    if recordCount > 0:
        df = df.drop('attributes', axis = 1)
    return (df,recordCount)

def inRestrictFromSeries(pdSeries):
	""" returns parentesized commma separated list from a pandas series
	Parameters
	_______
	pdSeries: Pandas series

	Returns
    __________
    String of form ('value1', 'value2'....) that can be concatenated
	with a constructed query. Retuns quoted elements for type object
	Fails on type date

	Todo
	_______
	Return nonquoted elements for type int and int64.
	_______
	"""
	datatype = pdSeries.dtype
	if datatype != 'O':
		raise NameError(pdSeries.name
            + ': unsupported dtype {}'.format(datatype))
	elements = list(pdSeries)
	tup_elements = tuple(elements)
	formatted_elements = str(tup_elements)
	return formatted_elements

def sqlInStrFromStruct(paramStruct, column = None):
    """ returns sqlstyle  "(1,2)" or "('a','b')"  fromm list

    Parameters
    _______
    paramStruct: python list


    Returns
    ________

    comma separated string of elements enclosed in parenthees

    """
    # Change incoming to a list
    if isinstance(paramStruct,list):
        l = paramStruct
    else:
        raise ValueError("Unsupported datatype: {}".format(incoming_type) )

    y = lambda l: str(tuple(l)).replace(',','') if len(l) == 1 else str(tuple(l))

    return y(paramStruct)

def getEventMasters(sfConnection, accountID):
    """ returns dataframe of EventMasters and count of EventMasters as tuple

    Parameters
    _______
    sfConnection: simple_salesforce Salesforce object
    accountID: Compass Account ID


    Returns
    ________
    Tuple of df_EventMasters (pandas dataframe), count (int)
    count is directly from salesforce API and should
    always agree with len(df_EventMasters)
    """


    qryEventMasters = "Select ID, Event_ID_Number__c\
    ,Name, Screening_Location__c, Screening_Date__c\
    ,Actual_Participation__c,RecordTypeId,Event_Type__c, Account_ID__c\
    ,related_event_master__c, CreatedDate, LastModifiedDate \
    FROM Screening_Event__c \
    WHERE Account__c = '{}'".format(accountID)

    df_EventMasters, count_EventMasters = SOQL(sfConnection,qryEventMasters)
    return(df_EventMasters,count_EventMasters)

def getEventMastersFromList(sfConnection, source_list, source_col='ID'):
    """ returns dataframe of Events Master and count of Events Master as tuple

    Parameters
    _______
    sfConnection: simple_salesforce Salesforce object
    source_list: iterable of parameter(s)
    source_col: str, target column to query, default = 'ID'


    Returns
    ________
    Tuple of df_EventMasters (pandas dataframe), count (int)
    count is directly from salesforce API and should
    always agree with len(df_EventMasters)
    """
    if type(source_list) not in [tuple,list,set]:
        raise ValueError(f'source_list is not one of tuple,list,set')

    instring='\',\''.join(map(str,source_list))
    instring = "('" + instring
    instring += "')"

    restricted_field = source_col
    qryEventMasters = f"Select ID, Account__c, Event_ID_Number__c\
    ,Clinic_ID__C ,Name, Screening_Location__c, Screening_Date__c\
    ,Actual_Participation__c,Event_Type__c,  related_event_master__c \
    FROM  Screening_Event__c  \
    WHERE {restricted_field} in {instring} "


    df_EventMasters, count_EventMasters = SOQL(sfConnection,qryEventMasters)
    return(df_EventMasters,count_EventMasters)

def getMXW_UsersFromContact(sfConnection, contacts):
    """ returns dataframe of Users and count of Users as tuple

    Parameters
    _______
    sfConnection: simple_salesforce Salesforce object
    contacts: Tuple of mxw contacts


    Returns
    ________
    Tuple of df_Users (pandas dataframe), count (int)
    count is directly from salesforce API and should
    always agree with len(df_Users)
    """
    if contacts == None:
        raise ValueError('Must provide tuple of contact or contacts')
    if  type(contacts) not in [tuple,list,set]:
        raise ValueError(f'contacts is not one of tuple,list')


    instring='\',\''.join(map(str,contacts))
    instring = "('" + instring
    instring += "')"

    qryUsers = f"SELECT Compass_Contact_SFDCID__c,ContactId,Contact_Wellness_ID__c\
    ,CreatedDate,Email,EmployeeNumber,FederationIdentifier,FirstName,Id,IsActive,\
    IsPortalEnabled,LastLoginDate,LastModifiedDate,LastName FROM User \
    WHERE ContactID in {instring}"


    df_Users, count_Users = SOQL(sfConnection,qryUsers)
    return(df_Users,count_Users)

def getEventDetails(sfConnection, event_masters=None, contacts=None, fields=None):
    """ returns dataframe of EventDetails and count of EventDetails as tuple

    Parameters
    _______
    sfConnection: simple_salesforce Salesforce object
    event_masters: tuple of event masterse
    contacts: tuple of contacts
    fields: list of fields to return default None
    (returns basic set of data)

    One and only one of contacts and event_masters must populated


    Returns
    ________
    Tuple of df_EventDetails (pandas dataframe), count (int)
    count is directly from salesforce API and should
    always agree with len(df_EventDetails)
    """

    if event_masters == None and contacts == None:
        raise ValueError('Must provide tuple of event_masters or contacts')
    if type(event_masters) not in [tuple,list,set,None]\
        and type(contacts) not in [tuple,list,set,None]:
        raise ValueError(f'Neither {contacts} {type(contacts)} nor {event_masters} {type(event_masters)} is one of tuple,list, set')
    if type(event_masters) in [tuple,list,set]\
        and type(contacts) in [tuple,list,set]:
        raise ValueError(f'Either event_masters or contacts can be passed in but not both')

    if contacts == None:
        instring='\',\''.join(map(str,event_masters))
        instring = "('" + instring
        instring += "')"
        restricted_field = 'Screening_Event__c'
    else:
        instring='\',\''.join(map(str,contacts))
        instring = "('" + instring
        instring += "')"
        restricted_field = 'Contact__r.ID'

    qryEventDetails_where = f' WHERE {restricted_field} in {instring}'
    if fields == None:

        qryEventDetails_fields = f"Select Name,Screening_Event__r.Name,\
        Screening_Event__c,Screening_Date__c, Contact__c,Biometric_Date__c,\
        Reimbursement_Status__c, PortalMasterKey__c, RecordTypeIdType__c,\
        Maxwell_URL__c, File_Name__c, MXW_QRG_SFDCID__c \
        FROM Screening_Detail__c"
    else:
        qryEventDetails_fields = '\',\''.join(map(str,fields))
        qryEventDetails_fields = "'" + qryEventDetails_fields
        qryEventDetails_fields += "' FROM Screening_Detail__c"

    qry=qryEventDetails_fields + qryEventDetails_where

    df_EventDetails, count_EventDetails = SOQL(sfConnection,qry)
    return(df_EventDetails,count_EventDetails)

def getContacts(sfConnection, accountID, include_staff_created = False):
    """ returns dataframe of contacts and count of contacts as tuple

    Parameters
    _______
    sfConnection: simple_salesforce Salesforce object
    accountID: str, Compass Account ID
    include_staff_created: boolean, default=False


    Returns
    ________
    Tuple of df_contacts (pandas dataframe), count (int)
    count is directly from salesforce API and should
    always agree with len(df_Contacts)
    """

    if include_staff_created in [False,True]:

        qryContacts = f"Select ID,AccountID, FirstName, LastName, User_Name__C\
        ,MXW_SFDC_ID__c, Relationship__c, Contact_Type__c, Birthdate\
        ,Account_Code__C, Gender__c, Client_Employee_Number__c, PUID__c\
        ,Wellness_ID__c, load_date__c, CreatedDate\
        ,Employee_Status__c, SSN__c, Subscriber_ID__c\
        , Staff_Created_Contact__c \
        FROM Contact \
        WHERE Staff_Created_Contact__c = \
        {str(include_staff_created).upper()} and AccountID = '{accountID}'"
    else:
        ValueError(f"include_staff_created must be one of boolean True, False")


    df_contacts, count_contacts = SOQL(sfConnection,qryContacts)
    return(df_contacts,count_contacts)

def getMaxwellContacts(sfConnection, active_only = False, contacts=None, fields=None):
    """ returns dataframe of Maxwell Contacts and count of Contacts as tuple

    Parameters
    _______
    sfConnection: simple_salesforce Salesforce object
    active_only: restrict on mxw__Is_Active__c field.
        boolean, default=False

    contacts: tuple, list or set of Maxwell Contact IDs,
        default = None
        None returns all contacts
    fields: list of fields to return, Default None
        None returns the following fields: ID, mxw__Employee_Username__c,
        Compass_SFDCID__c, PUID__c, FirstName,LastName, Birthdate,  Gender__c
         Load_Date__c, mxw__Is_Active__c, mxw__Type__c, Wellness_Id__c



    Returns
    ________
    Tuple of df_Contacts (pandas dataframe), count (int)
    count is directly from salesforce API and should
    always agree with len(df_Contacts)
    """

    if contacts != None and type(contacts) not in [tuple,list,set]:
        raise ValueError(f'contacts is not one of tuple,list, set')
    if fields != None and type(fields) not in [tuple,list,set]:
        raise ValueError(f'fields is not one of tuple,list, set')

    if contacts != None:
        restricted_clause = " WHERE id in ('"
        restricted_clause += '\',\''.join(map(str,contacts))
        restricted_clause += "')"
    else:
        restricted_clause = ''



    if fields == None:
        qryContact_fields = f"Select ID, mxw__Employee_Username__c,\
        Compass_SFDCID__c, PUID__c, FirstName,LastName, Birthdate,  Gender__c,\
         Load_Date__c, mxw__Is_Active__c, mxw__Type__c, Wellness_Id__c"
    else:
        qryContact_fields = 'Select '
        qryContact_fields += ','.join(map(str,fields))

    qry=qryContact_fields + ' FROM Contact' + restricted_clause

    df_contacts, count_contacts = SOQL(sfConnection,qry)
    return(df_contacts,count_contacts)

def getContactsFromList(sfConnection, source_list, source_col='ID'
    , include_staff_created = False, address_info = False):
    """ returns dataframe of contacts and count of contacts as tuple

    Parameters
    _______
    sfConnection: simple_salesforce Salesforce object
    source_list: iterable of parameter(s)
    source_col: str, target column to query, default = 'ID'
    include_staff_created: boolean, default=False
    address_info: boolean, default=False. Add Mailing Fields


    Returns
    ________
    Tuple of df_contacts (pandas dataframe), count (int)
    count is directly from salesforce API and should
    always agree with len(df_Contacts)
    """
    if type(source_list) not in [tuple,list,set]:
        raise ValueError(f'source_list is not one of tuple,list,set')

    instring='\',\''.join(map(str,source_list))
    instring = "('" + instring
    instring += "')"

    field_list = "Select ID, AccountID, FirstName, LastName, User_Name__c\
        ,MXW_SFDC_ID__c, Relationship__c, Contact_Type__c, Birthdate\
        ,Account_Code__c, Gender__c, Client_Employee_Number__c, PUID__c\
        ,Wellness_ID__c, load_date__c, CreatedDate,Employee_Status__c\
        ,SSN__c, Subscriber_ID__c, Staff_Created_Contact__c"
    if address_info == True:
        field_list = field_list + ",MailingStreet, MailingCity, MailingState,\
        MailingPostalCode"

    if include_staff_created in [False,True]:

        qryContacts = f"{field_list} \
        FROM Contact \
        WHERE Staff_Created_Contact__c = \
        {str(include_staff_created).upper()} and {source_col} in {instring}"
    else:
        ValueError(f"include_staff_created must be one of boolean True, False")


    df_contacts, count_contacts = SOQL(sfConnection,qryContacts)
    return(df_contacts,count_contacts)

def spouseIDFromEmployee( MemberLast, MemberID, SpouseLast, SpouseDOB, dfXref,maxChars=25):

    """ returns spouses identifier by matching member and spouse data to xref

    Parameters
    _______
    MemberLast: Member LastName from data share
    MemberID: Key for lookups from data share
    SpouseLast: Spouse LastName from data share
    SpouseDOB: Spouse DOB from data share
    dfXref: Current EE data (spouse and employee on one row) as Pandas series::
    LastName_sps   Birthdate_sps   Sub_Employee_ID  SSN__c_emp  LastName_emp


    Returns
    ________
    Tuple of Match_Type (int), MatchSSN (string)
    Match_type values::
    None:  no match
    1: Match on DOB and Spouse Last
    2: More than one match
    """

    #Not Used: MatchLength = maxChars + 1

    #Rename spouse ssn column to return to subscriber id of calling dataframe
    dfXref.rename(columns = {'SSN__c_sps' : 'Sub_Employee_ID'}, inplace=True)
    #Matches and their counts of returned records
    #print(dfXref.columns)
    df_MatchLast = dfXref[(dfXref['LastName_emp'] == MemberLast)\
     & (dfXref['LastName_sps'] == SpouseLast)\
     & (dfXref['Birthdate_sps'] == SpouseDOB)]
    count_last = df_MatchLast.shape[0]

    #df_MatchID = dfXref[(dfXref['SSN__c_emp'])] == MemberID\
    # & (dfXref['Birthdate_sps']) == SpouseDOB
    #count_ID = df_MatchID.shape[0]
    count_ID = 0

    df_MatchEmpLast = dfXref[(dfXref['LastName_emp'] == MemberLast)\
     & (dfXref['Birthdate_sps'] == SpouseDOB)]
    count_EmpLast = df_MatchEmpLast.shape[0]

    score = 0
    found = set()
    #constants to track matches
    FOUND_MATCH_LAST = 1
    FOUND_MATCH_ID = 2
    FOUND_MATCH_EMP_LAST = 4

    if sum([count_last,count_ID,count_EmpLast]) == 0 \
    or sum([count_last,count_ID,count_EmpLast]) > 3:
    #Fail
        return (128,None)

    if count_last == 1:
        score += FOUND_MATCH_LAST
        found.add(df_MatchLast['Sub_Employee_ID'].to_string())

    if count_ID == 1:
        score += FOUND_MATCH_ID
        found.add(df_MatchID['Sub_Employee_ID'].to_string())

    if count_EmpLast == 1:
        score += FOUND_MATCH_EMP_LAST
        found.add(df_MatchEmpLast['Sub_Employee_ID'].to_string())

    #Fail if zero or greater than one matches
    if len(found) != 1:
        return (256,none)
    if score & FOUND_MATCH_LAST == FOUND_MATCH_LAST:
        #print(df_MatchLast.columns)
        return df_MatchLast['Sub_Employee_ID']

    elif score & FOUND_MATCH_ID == FOUND_MATCH_ID:
        return df_MatchID['Sub_Employee_ID']

    elif score & FOUND_MATCH_EMP_LAST == FOUND_MATCH_EMP_LAST:
        return df_MatchEmpLast['Sub_Employee_ID']
    else:
        raise Error('Recorded match but no value to return')

    #return matching method(s) and single returned SpouseID
    #return (score,found.pop())

def getResponsesFromQuestionSet(sfConnection, source_list, field_list=None):
    """ returns dataframe of contacts and count of contacts as tuple

    Parameters
    _______
    sfConnection: simple_salesforce Salesforce object
    source_list: iterable of parameter(s)
    field_list: list, specific field list, default = None


    Returns
    ________
    Tuple of df_Responses(pandas dataframe), count (int)
    count is directly from salesforce API and should
    always agree with len(df_Responses)
    """
    if type(source_list) not in [tuple,list,set]:
        raise ValueError(f'source_list is not one of tuple,list,set')



