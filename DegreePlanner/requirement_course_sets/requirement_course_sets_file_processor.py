'''
For Starfish Degree Planner - creates requirement_course_sets file.txt for ingestion into Starfish
Reads import_csv_file and outputs output_csv_file - import_csv_file is created by registrar.
genedcourses.csv should be derived from genedcourses.xlsx and catalog years should be kept seperated in the excel sheet to keep track
New set titles can be added by adding a new column to the CSV file and adding the label (ex. "WC-R") to the set_title dictionary

import_csv_file Example:
COURSE,COURSE COMP,LIBARTS,WC-F,QP-F,AR-F,RE-F,SC-F,WC-R,QP-R,AR-R,RE-R,SC-R,WC-I,QP-I,AR-I,RE-I,SC-I
Adirondack Expedition ,SOC 115,X,,,,,,,,,X,X,,,,,
Adirondack Field Ecology (summer),ENV 199,X,,,,,,,,X,,,,,,,
Adirondack Fungi and Radical Mycology,INT 399,X,,,,,,,,,,,,,,X,X
Adirondack Nature-Based Tourism ,REC 300,,,,,,,,,,,,,,,,X
Adirondack Studies (was Adirond Exped),SOC 115,X,,,,,,,,,,X,,,,,
Description needs to be less than 64 characters
Abreviation needs be less than 17
'''

import pandas as pd
import numpy as np
import datetime

#EDIT THESE VARIABLES APPROPRIATELY

today = datetime.datetime.now().strftime('%Y%m%d')
#catalog_year uses catalog_year in the naming convention of the files to keep them segregated
catalog_year = '2017'
#import_csv_file - this file is produced by exporting the appropriate csv from genedcourses.xlsx
#import_csv_file = '\\\\psc-data\\E\\Applications\\Starfish\Files\\workingfiles\\requirement_course_sets\\' + catalog_year + '_genedcourses.csv'
import_csv_file = catalog_year + '_genedcourses.csv'
#output_csv_file - this file is what is produced to be provided to starfish
#output_csv_file = '\\\\psc-data\\E\\Applications\\Starfish\Files\\workingfiles\\requirement_course_sets\\' + today + '_' + catalog_year + '_requirement_course_sets.txt'
output_csv_file = today + '_' + catalog_year + '_requirement_course_sets.txt'


set_titles = {
    'DEGREE_APPLICABL':'Degree Applicable',
    'LIBARTS':'Liberal Arts and Science',
    'WC-F':'Written Communication - Foundation',
    'QP-F':'Quantitative Problem Solving - Foundation',
    'AR-F':'Analytical Reasoning & Scientific Inquiry - Foundation',
    'RE-F':'Responsibility & Expression - Foundation',
    'SC-F':'Social & Cultural Engagement - Foundation',
    'WC-R':'Written Communication - Reinforcing',
    'QP-R':'Quantitative Problem Solving - Reinforcing',
    'AR-R':'Analytical Reasoning & Scientific Inquiry - Reinforcing',
    'RE-R':'Responsibility & Expression - Reinforcing',
    'SC-R':'Social & Cultural Engagement - Reinforcing',
    'WC-I':'Written Communication - Integrated',
    'QP-I':'Quantitative Problem Solving - Integrated',
    'AR-I':'Analytical Reasoning & Scientific Inquiry - Integrated',
    'RE-I':'Responsibility & Expression - Integrated',
    'SC-I':'Social & Cultural Engagement - Integrated',
    'BIOG_UD_BIO':'BIOG: Upper Division Biology Electives',
    'ENVS_UD_SCIENCE':'ENVS: Upper Division Science Electives',
    'COMM_COMMMETHODS':'COMM: Communication Methods Cluster',
    'COMM_DIVERSITY':'COMM: Diversity Cluster',
    'SCWL_SUSTAINABLE':'SCWL: Sustainable Practitioner Cluster',
    'SCWL_SOCIETY':'SCWL: Society and Natural World Foundation Course',
    'RECR_RECREATION':'RECR: Recreation Management Cluster',
    'RECR_DIVERSITY':'RECR: Diversity Cluster',
    'RECR_EXPERIENTIA':'RECR: Experiential Cluster',
    'RECR_NATURAL':'RECR: Natural World Cluster',
    'PACM_HUMAN':'PACM: Human Dimension Cluster', 
    'PACM_NATURAL':'PACM: Natural World Cluster',
    'NRCM_CULTURAL':'NRCM: Cultural Perspective Cluster',
    'NRCM_ECOSYSTEM':'NRCM: Ecosystem Management Cluster',
    'NRCM_NEGOTIATION':'NRCM: Negotiation/Planning Cluster',
    'NRCM_ORGANISMS':'NRCM: Organisms/Habitats Cluster',
    'NRCM_PRACTITIONE':'NRCM: Practitioner Skills Cluster',
    'NRCM_SOCIETY':'NRCM: Society and Natural World Foundation Course',
    'HRTM_CUSTOMER':'HRTM: Customer Relations Cluster',
    'HRTM_DIVERSITY':'HRTM: Diversity Cluster',
    'HRTM_MANAGEMENT':'HRTM: Management Cluster',
    'FWSW_ZOOLOGY':'FWSW - Wildlife Concentration: Zoology Elective',
    'FWSW_BOTANY':'FWSW - Wildlife Concentration: Botany Elective',
    'FWSW_ECOLOGY':'FWSW - Wildlife Concentration: Ecology Elective',
    'FWSW_PHYSICAL':'FWSW - Wildlife Concentration: Physical Science Elective',
    'FWSW_POLICY':'FWSW - Wildlife Concentration: Policy, Admin & Law Elective',
    'FWSW_WILDLIFE':'FWSW - Wildlife Concentration: Wildlife Biology Elective',
    'FWSF_BIOLOGICAL':'FWSW - Fisheries Concentration: Biological Science Elective',
    'FWSF_HUMAN':'FWSW - Fisheries Concentration: Human Dimension Elective',
    'FBIO_BIOLOGY':'Forestry - Biology Concentration: Biology Cluster',
    'FSMT_CUSTOMER':'FSMT: Customer Relations Cluster',
    'FSMT_DIVERSITY':'FSMT: Diversity Cluster',
    'FSMT_MANAGEMENT':'FSMT: Management Cluster',
    'ENST_ENV_HUMAN':'ENST: Environment & Human Expression Cluster',
    'ENST_ENV_SOCIETY':'ENST: Environment & Society Cluster',
    'ENST_ENV_SCIENCE':'ENST: Environment & Science Cluster',
    'ENST_PRACTITIONE':'ENST: Practitioner Skills Cluster',
    'ENST_SOCIETY':'ENST: Society and Natural World Foundation Course',
    'ECOR_HUMAN':'ECOR: Human System Cluster',
    'BASM_MANAGEMENT':'BASM: Management Cluster',
    'FORT_FORT':'FORT: Forest Technology Cluster',
    'IS_BASM':'Integrative Students: BASM Program Options',
    'IS_BASM_ONE':'Integrative Students: BASM Select One Program Options',
    'IS_BIOG':'Integrative Students: BIOG Program Options',
    'IS_COMM':'Integrative Students: COMM Program Options',
    'IS_CASM':'Integrative Students: CASM Program Options',
    'IS_CASM_ONE':'Integrative Students: CASM Select One Program Options',
    'IS_ECOR':'Integrative Students: ECOR Program Options',
    'IS_EBSB':'Integrative Students: EBSB Program Options',
    'IS_ENVS':'Integrative Students: ENVS Program Options',
    'IS_ENST':'Integrative Students: ENST Program Options',
    'IS_FOR':'Integrative Students: FOR Program Options',
    'IS_FW':'Integrative Students: FW Program Options',
    'IS_FSMT':'Integrative Students: FSMT Program Options',
    'IS_HRTM':'Integrative Students: HRTM Program Options',
    'IS_HRTM_ONE':'Integrative Students: HRTM Select One Program Options',
    'IS_MNGT':'Integrative Students: MNGT Program Options',
    'IS_NRCM':'Integrative Students: NRCM Program Options',
    'IS_PACM':'Integrative Students: PACM Program Options',
    'IS_PSYCH':'Integrative Students: PSYCH Program Options',
    'IS_RECR':'Integrative Students: RECR Program Options',
    'IS_SCWL':'Integrative Students: SCWL Program Options',
    'IS_AALM':'Integrative Students: AALM Program Options',
    'IS_SURV':'Integrative Students: SURV Program Options'


	
}



import csv
with open (import_csv_file) as csvfile:
    CatalogYear = '2017'
    freader = csv.reader(csvfile, delimiter=',', quotechar = '"')
    courses = []
    count = 0
    lines = []
    abbreviation_names = []
    for row in freader:
        if count == 0:
            for x in range(0, len(row)):
                abbreviation_names.append(row[x])
            print(abbreviation_names)
        else:
            CourseID = row[1].replace(' ','')
            for x in range(0, len(row)):
                if row[x] == 'X':
                    lines.append([CourseID,CatalogYear,abbreviation_names[x],set_titles[abbreviation_names[x]]])
                    
        
        count += 1
headers = ['course_integration_id','catalog_year','set_abbreviation','set_title']
df = pd.DataFrame.from_records(lines, columns = headers)
#print df

sorteddf = df.sort_values(by=['set_abbreviation','set_title'])
print(sorteddf)

sorteddf.to_csv(output_csv_file, sep=',', index = False)

#OutputFile = open(output_csv_file, 'wb')
#with OutputFile:
#    writer = csv.writer(OutputFile)
#    writer.writerows(lines)
         
print("Writing complete")
