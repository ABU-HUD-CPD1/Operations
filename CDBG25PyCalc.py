# -*- coding: utf-8 -*-
"""
Created on Tue Aug 13 15:35:41 2024

@author: H57056
"""

##########################################################
###########################################################
##     HUD/CPD/OPS/SDED: CDBG Eligibility in Python!     ##
##        Written by Abu Zuberi, October 2022            ##
##        Updated by Abu Zuberi, July 2024                ##
##                     ❀♡❀⊱✿⊰❀♡❀                         ##
##     This program takes CPD-SDED's "MERGE" database    ##
##         and Census data to output CDBG eligibility    ##
##                          lists.                       ##
###########################################################
###########################################################


## User Input (): Set Output Folder/Filename:
OutputFolder = 'J:/COMS/CDBG/CDBG25/New Eligibility/'
OutputFilename = 'CDBG_Cities_Report.xlsx'
OutputFilenameCounties = 'CDBG_Counties_Report.xlsx'

## User Input () MERGE file
MERGELocation = 'J:/COMS/MERGE/MERGE FY25/MERGE.xlsx'
MERGEPopYear= "POP21" #Not used
RequalificationCycle= "1" #not used

## User Input (): Set Census Population Data:
POPDataYearCurrent= "POPESTIMATE2023"

#Note this program won’t work if the Census population estimates file doesn’t have leading zeroes on the FIPS codes.

## User Input (): Set Census Delineation Files MSA(/Div), Principal Cities, Population Estimate Locations
#"https://www2.census.gov/programs-surveys/metro-micro/geographies/reference-files/2020/delineation-files/list1_2020.xls"
#"https://www2.census.gov/programs-surveys/metro-micro/geographies/reference-files/2020/delineation-files/list2_2020.xls"
CensusMSAxCountyFileLocation='J:/COMS/Data and Program Library/OMB Data/2023 July/list1_2023.xlsx'
CensusPrincipalCitiesFileWebLocation= 'J:/COMS/Data and Program Library/OMB Data/2023 July/list2_2023.xlsx'
CensusPOPWebLocation= 'J:/COMS/POP/2023/sub-est2023.csv'

## User Input CPD Field Office Data FileS Location  #These locations shouldn’t need updating every year.
CPDFOxCountyFileLocation='J:/COMS/Data and Program Library/CPD Field Office Data/Field Office By County.xlsx'
CPDFieldOfficeNamesLocation = 'J:/COMS/Data and Program Library/CPD Field Office Data/Field Office Codes.xlsx'

# Most of the MCDs in twelve states (Connecticut, Maine, Massachusetts, Michigan, Minnesota, New Hampshire, New Jersey, New York, Pennsylvania, Rhode Island, Vermont, and Wisconsin) serve as general-purpose local governments:
MCDStates= ("CT","ME","MA","MI", "MN", "NH","NJ", "NY","PA","RI","VT","WI")
MCDStateFIPS=('09','23','25','26','35','33','34','36','42','44','50','55',)
StatesNoCountyGov = ("CT", "HI", "MA", "RI")
StatesNoCountyGovFIPS = ("09", "15", "44", "25")

# this list excludes Nashville-Davidson metropolitan government (balance), TN; Washington city, DC; Athens-Clarke County unified government (balance), GA; Augusta-Richmond County consolidated government (balance), GA; Columbus city, GA; Urban Honolulu CDP, HI; Indianapolis city (balance), IN; Lexington-Fayette urban county, KY; Louisville/Jefferson County metro government (balance), KY; Baton Rouge city, LA; Houma city, LA; Lafayette city, LA; Baltimore city, MD
CitiesToSkip= ('4752006','1150000','1303440','1304204','1319000','1571550','1836003','2146027','2148006','2205000','2236255','2240735','2404000', '2203399990', "2205599990")

# import Pandas numerical analysis plug-ins, import time for timer:
import time
start_time = time.time()
import pandas as pd
# Tell Pandas not to truncate when printing:
pd.options.display.max_columns = 5
pd.options.display.max_rows = 5

# Import MERGE
MERGE=(pd.DataFrame(pd.read_excel(MERGELocation,converters={'CDBGTY':str,'CDBGHPL':str,'ST':str,'CO':str, 'UC':str,'MA':str,'RGN':str,'FO':str,'CYCLE':str, 'AGL':str})))[(pd.DataFrame(pd.read_excel(MERGELocation,converters={'CDBGTY':str,'CDBGHPL':str,'ST':str,'CO':str, 'UC':str,'MA':str,'RGN':str,'FO':str,'CYCLE':str, 'AGL':str})))['CDBGFLAG']=="C"]
## DATA CORRECT MERGE
MERGE.loc[MERGE['MCD'] == "00000",'MCD']="99999"
MERGE.loc[MERGE['PLACE'] == "00000",'PLACE']="99999"
MERGE['UCKEY']=MERGE['ST']+MERGE['CDBGHPL']
MERGE['Note'] = MERGE['CDBGTY']
MERGE['UCName'] = "Participates with "+MERGE['NAME']
MERGE['UCKEY'] = MERGE["ST"]+MERGE["CDBGHPL"]
MERGE.loc[MERGE['PLACE'] == MERGE['MCD'],'MCD']="99999"

# Import MSA x County and PC lists from Census. SKIP first two rows on MSAxCounty and Principal Cities lists because Census didn't put anything there.
MSAxCounty = pd.read_excel(CensusMSAxCountyFileLocation, converters={'CBSA Code':str,'Metropolitan Division Code':str,'FIPS State Code':str,'FIPS County Code':str}, skiprows=[0,1])
CensusPrincipalCitiesList=pd.read_excel(CensusPrincipalCitiesFileWebLocation, converters={'CBSA Code':str,'FIPS State Code':str,'FIPS Place Code':str}, skiprows=[0,1])
CensusPOP= pd.read_csv(CensusPOPWebLocation,converters={'SUMLEV':str,'STATE':str,'COUNTY':str,'PLACE':str,'COUSUB':str,'CONCIT':str,'PRIMGEO_FLAG':str},engine='python',encoding='latin1')

#Assign Fields
MSAxCounty = MSAxCounty.assign(MA="",STATECOUNTY="")
CensusPrincipalCitiesList = CensusPrincipalCitiesList.assign(FIPSKEY="", PCFlag="PC")
CensusPOP = CensusPOP.assign(MA="", FIPSKEY="", STATECOUNTY="")
CensusPOP["POP"] = CensusPOP[POPDataYearCurrent] 
CensusPOP = CensusPOP.astype({'POP':int})

# Update Fipskeys
CensusPOP.loc[CensusPOP['SUMLEV'] == '040', ['FIPSKEY']] = CensusPOP["STATE"]
CensusPOP.loc[CensusPOP['SUMLEV'] == '050', ['FIPSKEY']] = CensusPOP["STATE"]+CensusPOP["COUNTY"]
CensusPOP.loc[CensusPOP['SUMLEV'] == '061', ['FIPSKEY']] = CensusPOP["STATE"]+CensusPOP["COUNTY"]+CensusPOP["COUSUB"]
CensusPOP.loc[CensusPOP['SUMLEV'] == '071', ['FIPSKEY']] = CensusPOP["STATE"]+CensusPOP["COUNTY"]+CensusPOP["PLACE"]
CensusPOP.loc[(CensusPOP['SUMLEV'] == '071')&(CensusPOP['PLACE'] == '99990'), ['FIPSKEY']] = CensusPOP["STATE"]+CensusPOP["COUNTY"]+CensusPOP["COUSUB"]+CensusPOP["PLACE"]
CensusPOP.loc[CensusPOP['SUMLEV'] == '157', ['FIPSKEY']] = CensusPOP["STATE"]+CensusPOP["COUNTY"]+CensusPOP["PLACE"]
CensusPOP.loc[CensusPOP['SUMLEV'] == '162', ['FIPSKEY']] = CensusPOP["STATE"]+CensusPOP["PLACE"]
CensusPOP.loc[CensusPOP['SUMLEV'] == '170', ['FIPSKEY']] = CensusPOP["STATE"]+CensusPOP["CONCIT"]
CensusPOP['STATECOUNTY'] = CensusPOP["STATE"]+CensusPOP["COUNTY"]
MSAxCounty['STATECOUNTY'] = MSAxCounty["FIPS State Code"]+MSAxCounty["FIPS County Code"]

#Import FO Data
CPDFOxCounty=pd.DataFrame(pd.read_excel(CPDFOxCountyFileLocation,converters={'ROFO':str, 'STFIPS':str, 'COUNTY':str}))
CPDFieldOfficeNames=pd.DataFrame(pd.read_excel(CPDFieldOfficeNamesLocation,converters={'ROFO':str}))
#Add FO Data

CPDFOxCounty["STATE"]=CPDFOxCounty["STFIPS"]
CensusPOP=  pd.merge(CensusPOP, CPDFOxCounty.loc[:,['STATE', 'COUNTY', "ROFO", "STA"]],  how='left',  left_on=['STATE', 'COUNTY'], right_on = ['STATE', 'COUNTY']).fillna("").drop_duplicates()
CensusPOP=  pd.merge(CensusPOP, CPDFieldOfficeNames.loc[:,['ROFO', 'NameProper']],  how='left',  left_on=['ROFO'], right_on = ['ROFO']).fillna("").drop_duplicates()

#Add MSA codes  
MSAxCounty.drop(MSAxCounty[MSAxCounty['Metropolitan/Micropolitan Statistical Area']== "Micropolitan Statistical Area"].index, inplace = True)
MSAxCounty.drop(MSAxCounty[MSAxCounty['Metropolitan/Micropolitan Statistical Area'].isna()].index, inplace = True)
MSAxCounty=MSAxCounty.fillna("")
(MSAxCounty.loc[MSAxCounty['Metropolitan Division Code'] != '', ['CBSA Code']])= MSAxCounty['Metropolitan Division Code']
(MSAxCounty.loc[MSAxCounty['Metropolitan Division Title'] != '', ['CBSA Title']])= MSAxCounty['Metropolitan Division Title']

CensusPOP= pd.merge(CensusPOP, MSAxCounty[['FIPS State Code','FIPS County Code', 'CBSA Code', 'CBSA Title']],  how='left',  left_on=['STATE', 'COUNTY'], right_on = ['FIPS State Code','FIPS County Code', ]).fillna("").drop_duplicates()

#Find Entitled Cities in MERGE
MetroCities = MERGE.loc[(MERGE['CDBGTY'] == "51")|(MERGE['CDBGTY'] == "52")]

#Create MERGE Urban County List:
MERGEUCLIST = MERGE.loc[(MERGE['CDBGTY'] == "61")&(MERGE['CDBGFLAG'] == "C")]
PlacesInUCs = MERGE.loc[(MERGE['CDBGTY'] == "64")&(MERGE['UC'] == "1")|(MERGE['CDBGTY'] == "63")&(MERGE['UC'] == "4")]
TotPlacesInUCs= PlacesInUCs.loc[ PlacesInUCs['CDBGTY'] == "63"]
PPlacesInUCs=PlacesInUCs.loc[ PlacesInUCs['CDBGTY'] == "64"]

# Identify Potential Cities
CensusXMerge = CensusPOP
CensusXMerge = CensusXMerge.loc[(CensusXMerge['PRIMGEO_FLAG'] == "1")]

# ADD PLACE POPULATION TOTALS
PlacePopTots= CensusPOP.loc[(CensusPOP['SUMLEV'] == "162")]
PlacePopTots= pd.merge(CensusXMerge[['STATE', 'PLACE']], PlacePopTots,  how='left',  left_on=['STATE', 'PLACE'], right_on = ['STATE', 'PLACE']).fillna("")
PlacePopTots["PopTot"]= PlacePopTots["POP"]
PlacePopTots["PLACEKEY"]= PlacePopTots["FIPSKEY"]
PlacePopTots.drop(PlacePopTots[PlacePopTots['PLACE'] == "99990"].index, inplace = True)
PlacePopTots.drop(PlacePopTots[PlacePopTots['PLACE'] == "00000"].index, inplace = True)
CensusXMerge= pd.merge(CensusXMerge, PlacePopTots[['STATE', 'PLACE', "PopTot", "PLACEKEY"]],  how='left',  left_on=['STATE', 'PLACE'], right_on = ['STATE', 'PLACE']).drop_duplicates()

#Make MCD totals to themself
CensusXMerge["PopTot"]=CensusXMerge["PopTot"].fillna(0)
CensusXMerge.loc[CensusXMerge["SUMLEV"]=="061", ["PopTot"]] += CensusXMerge.loc[CensusXMerge["SUMLEV"]=="061", ["POP"]].to_numpy()
CensusXMerge["PLACEKEY"]=CensusXMerge["PLACEKEY"].fillna("")
CensusXMerge.loc[CensusXMerge["SUMLEV"]=="061", ["PLACEKEY"]]= CensusXMerge.loc[CensusXMerge["SUMLEV"]=="061", ["FIPSKEY"]].to_numpy()

# ADD County NAmes 
CountyNames= CensusPOP.loc[(CensusPOP['SUMLEV'] == "050")]
CountyNames = CountyNames.rename(columns={"NAME":"CountyName"})
CensusXMerge= pd.merge(CensusXMerge, CountyNames[['STATE', 'COUNTY', "CountyName"]],  how='left',  left_on=['STATE', 'COUNTY'], right_on = ['STATE', 'COUNTY']).drop_duplicates()

# Match and note Cities in MERGE
MatchedbyPlaceCode= pd.merge(CensusXMerge, MetroCities.loc[:,['ST', 'PLACE', "Note"]],  how='left',  left_on=['STATE', 'PLACE'], right_on = ['ST', 'PLACE']).fillna("").drop_duplicates()
MatchedbyMCD= pd.merge(CensusXMerge, MetroCities.loc[:,['ST', 'CO', 'MCD', "Note"]],  how='left',  left_on=['STATE', 'COUNTY', 'COUSUB'], right_on = ['ST','CO', 'MCD']).fillna("").drop_duplicates()
CensusXMerge["Note"] = MatchedbyPlaceCode["Note"].to_numpy()
CensusXMerge["Note"] += MatchedbyMCD["Note"].to_numpy()
(CensusXMerge.loc[(CensusXMerge['Note'].str.len()==4), ['Note']]) =   CensusXMerge['Note'].str[-2:]
    
# Find places participating Urban Counties###########
MatchedbyMCD= pd.merge(CensusXMerge, PlacesInUCs[['ST', 'CO', 'MCD', "UCKEY", "CYCLE"]],  how='left',  left_on=['STATE', 'COUNTY', 'COUSUB'], right_on = ['ST','CO', 'MCD']).fillna("").drop_duplicates()
MatchedbyPPlaceCode= pd.merge(CensusXMerge, PPlacesInUCs[['ST','CO', 'PLACE', "UCKEY", "CYCLE"]],  how='left',  left_on=['STATE', 'COUNTY', 'PLACE'], right_on = ['ST','CO', 'PLACE']).fillna("").drop_duplicates()
MatchedbyTotPlaceCode= pd.merge(CensusXMerge, TotPlacesInUCs[['ST', 'PLACE', "UCKEY", "CYCLE"]],  how='left',  left_on=['STATE',  'PLACE'], right_on = ['ST', 'PLACE']).fillna("").drop_duplicates()
CensusXMerge['UCName'] = MatchedbyPPlaceCode["UCKEY"].to_numpy()
CensusXMerge['UCName'] += MatchedbyTotPlaceCode["UCKEY"].to_numpy()
CensusXMerge['UCName'] += MatchedbyMCD["UCKEY"].to_numpy()
CensusXMerge['CYCLE'] = MatchedbyPPlaceCode["CYCLE"].to_numpy()
CensusXMerge['CYCLE'] += MatchedbyTotPlaceCode["CYCLE"].to_numpy()
CensusXMerge['CYCLE'] += MatchedbyMCD["CYCLE"].to_numpy()
(CensusXMerge.loc[(CensusXMerge['UCName'].str.len()==12), ['UCName']]) =   CensusXMerge['UCName'].str[-6:]
(CensusXMerge.loc[(CensusXMerge['CYCLE'].str.len()==2), ['CYCLE']]) =   CensusXMerge['CYCLE'].str[-1:]
## Add Urban County Names
CensusXMerge['UCName']= CensusXMerge['UCName'].fillna("")
for x in CensusXMerge.loc[CensusXMerge['UCName']!="", "UCName"]:
    CensusXMerge.loc[CensusXMerge['UCName']== x, "UCName"] = (MERGE[(MERGE['UCKEY']== x)&(MERGE['CDBGTY']=='61')]["NAME"].values.item())

# Mark PRincipal Cities
CensusPrincipalCitiesList['PLACEKEY']= CensusPrincipalCitiesList['FIPS State Code']+CensusPrincipalCitiesList['FIPS Place Code']
CensusPrincipalCitiesList.drop(CensusPrincipalCitiesList[CensusPrincipalCitiesList['PLACEKEY'].isnull()].index, inplace = True)
CensusPrincipalCitiesList.drop(CensusPrincipalCitiesList[CensusPrincipalCitiesList['Metropolitan/Micropolitan Statistical Area']== "Micropolitan Statistical Area"].index, inplace = True)
CensusXMerge= pd.merge(CensusXMerge, CensusPrincipalCitiesList[['PLACEKEY' ,"PCFlag"]],  how='left',  left_on=['PLACEKEY'], right_on = ['PLACEKEY']).drop_duplicates()

for x in CitiesToSkip:
    CensusXMerge.loc[(CensusXMerge['PLACEKEY'] == x), ["Note"]] = "MC"
    CensusXMerge.loc[(CensusXMerge['FIPSKEY'] == x), ["Note"]] = "MC"

##Clean up Cities list
EligibleCities = CensusXMerge
EligibleCities = EligibleCities[(EligibleCities['PLACE'] != '99990')]
EligibleCities.loc[(EligibleCities["PopTot"]==EligibleCities["POP"])&(EligibleCities["CBSA Code"]==""), "Note"]= "Skip" #skip places totally not in MSA areas
EligibleCities["NAME"] =EligibleCities["NAME"].str.replace(r"\(.*\)","") # replace (pt.) with blank. need to update
EligibleCities.drop(EligibleCities[EligibleCities['Note']!= ""].index, inplace = True) # drop records where note <> ""
EligibleCities = EligibleCities.loc[(EligibleCities['PopTot'] > 49999)&(EligibleCities['POP'] > 0)|(EligibleCities['PCFlag']=="PC")&(EligibleCities['POP'] >0) ] # find records that are PC or pop greater than 50k 
EligibleCities.drop(EligibleCities[EligibleCities['SUMLEV'] == "050"].index, inplace = True) # drop county records
EligibleCities.drop(EligibleCities[EligibleCities['Note'] == "Skip"].index, inplace = True) # drop records marked skip
EligibleCities["FOName"]=EligibleCities["NameProper"]
EligibleCities["FIPSKEY"]=EligibleCities["PLACEKEY"]
EligibleCities = EligibleCities[["NAME","STA", "POP", "PopTot", "PCFlag","CountyName", "CBSA Title" ,"STNAME","FOName",  "UCName", "CYCLE","FIPSKEY", "SUMLEV",  "STATE", "COUNTY", "COUSUB", "PLACE","CBSA Code","ROFO",]]
EligibleCities = EligibleCities.sort_values(['ROFO', 'STA', "NAME"], ascending = [True, True, True])

### Mark Current Urban Counties
PotentialCounties= CensusXMerge
for x in MERGE.loc[MERGE['CDBGTY']=="61",["ST", "CO"]].to_numpy():   # kinda slow  this matches on multiple criteria and updates
    PotentialCounties.loc[PotentialCounties[['STATE', 'COUNTY']].isin(x).all(axis=1),["Note"]]= "Urban County"
for x in StatesNoCountyGovFIPS:
    PotentialCounties.loc[(PotentialCounties['STATE'] == x), ["Note"]] = "NOCOUNTYGOV"
PotentialCounties= PotentialCounties.loc[(PotentialCounties['CBSA Code'] != "")]
PotentialCounties["FOName"]=PotentialCounties["NameProper"]
for x in set(PotentialCounties["STATECOUNTY"]):  ## using "set" to aggregate speeds this up
    PotentialCounties.loc[PotentialCounties["STATECOUNTY"]==x, ["CountyPop"]]=(PotentialCounties.loc[PotentialCounties["STATECOUNTY"]==x, ["POP"]].sum()).to_numpy()
PotentialCounties.loc[PotentialCounties["PLACE"]=="99990", ["PopTot"]]=PotentialCounties.loc[PotentialCounties["PLACE"]=="99990", ["POP"]].to_numpy()
PotentialCounties.drop(PotentialCounties[PotentialCounties['CountyPop'] < 200000].index, inplace = True) # drop county records
PotentialCounties.drop(PotentialCounties[PotentialCounties['Note'] =="Urban County"].index, inplace = True) # drop county records
PotentialCounties.drop(PotentialCounties[PotentialCounties['Note'] =="NOCOUNTYGOV"].index, inplace = True) # drop county records
PotentialCounties = PotentialCounties[["CountyName", "STA","CountyPop",  "CBSA Title" , "STNAME", "FOName",  "NAME","POP", "PopTot", "Note", "UCName", "CYCLE","FIPSKEY", "SUMLEV", "STATE", "COUNTY","STATECOUNTY","COUSUB", "PLACE","CBSA Code","ROFO",]]
PotentialCounties = PotentialCounties.sort_values(['ROFO', 'STA', "FIPSKEY"], ascending = [True, True, True])

import os
#os.remove(OutputFolder+OutputFilename)  # delete existing file
#os.remove(OutputFolder+OutputFilenameCounties)  # delete existing file
EligibleCities.to_excel(OutputFolder+OutputFilename, index=False, sheet_name='CDBGCitiesReport')
PotentialCounties.to_excel(OutputFolder+OutputFilenameCounties, index=False, sheet_name='CDBGCountiesReport')

#Open file
#os.system("open -a 'Microsoft Excel.app' '%s'" % OutputFolder+OutputFilename) # open file

print("Runtime~ %s seconds" % round((time.time() - start_time),3))

# END OF THE PYTHON SCRIPT
