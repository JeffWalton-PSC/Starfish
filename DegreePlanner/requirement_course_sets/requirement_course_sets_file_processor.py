'''
For Starfish Degree Planner - creates requirement_course_sets file.txt for ingestion into Starfish
Reads import_csv_file and outputs output_csv_file - import_csv_file is created by registrar.
genedcourses.csv should be derived from genedcourses.xlsx and catalog years should be kept seperated in the excel sheet to keep track
New set titles can be added by adding a new column to the CSV file and adding the label and title to the first 2 rows 

import_csv_file Example:
COURSE,COURSE COMP,DEGREE_APPLICABL,LIBARTS,WC-F,QP-F,AR-F,RE-F,SC-F,WC-R,QP-R,AR-R,RE-R,SC-R,WC-I,QP-I,AR-I,RE-I,SC-I,BIOG_UD_BIO,ENVS_UD_SCIENCE,HHAE_ECO,HHAE_POLICY,COMM_COMMMETHODS,COMM_DIVERSITY,SCWL_SUSTAINABLE,SCWL_SOCIETY,RECR_RECREATION,RECR_DIVERSITY,RECR_EXPERIENTIA,RECR_NATURAL,PACM_HUMAN,PACM_NATURAL,NRCM_CULTURAL,NRCM_ECOSYSTEM,NRCM_NEGOTIATION,NRCM_ORGANISMS,NRCM_PRACTITIONE,NRCM_SOCIETY,HRTM_CUSTOMER,HRTM_DIVERSITY,HRTM_MANAGEMENT,FWSW_ZOOLOGY,FWSW_BOTANY,FWSW_ECOLOGY,FWSW_PHYSICAL,FWSW_POLICY,FWSW_WILDLIFE,FWSF_BIOLOGICAL,FWSF_HUMAN,FBIO_BIOLOGY,FSMT_CUSTOMER,FSMT_DIVERSITY,FSMT_MANAGEMENT,ENST_ENV_HUMAN,ENST_ENV_SOCIETY,ENST_ENV_SCIENCE,ENST_PRACTITIONE,ENST_SOCIETY,ECOR_HUMAN,BASM_MANAGEMENT,FORT_FORT,IS_BASM,IS_BASM_ONE,IS_BIOG,IS_COMM,IS_CASM,IS_CASM_ONE,IS_ECOR,IS_EBSB,IS_ENVS,IS_ENST,IS_FOR,IS_FW,IS_FSMT,IS_HRTM,IS_HRTM_ONE,IS_MNGT,IS_NRCM,IS_PACM,IS_PSYCH,IS_RECR,IS_SCWL,IS_AALM,IS_SURV,HRTM_SPAN,HRTM_FREN,HRTM_ITAL,SCM_PRAC,SCM_CLUSTER,CAPSTONE,MNGT_CORE,MNGT_ENTR,MNGT_SPORTS,BIO_MNR_UDELEC,BOT_MNR_ELEC,CBM_MNR_ELEC,EBM_MNR_ELEC,ECM_MNR_EI,ECM_MNR_C,ESM_MNR_ELEC,FRM_MNR_F,GIS_MNR_ELEC,MPP_MNR_ELEC,SCM_MNR_SP,SCM_MNR_ELEC
COURSE,COURSE COMP,Degree Applicable,Liberal Arts and Science,Written Communication - Foundation,Quantitative Problem Solving - Foundation,Analytical Reasoning & Scientific Inquiry - Foundation,Responsibility & Expression - Foundation,Social & Cultural Engagement - Foundation,Written Communication - Reinforcing,Quantitative Problem Solving - Reinforcing,Analytical Reasoning & Scientific Inquiry - Reinforcing,Responsibility & Expression - Reinforcing,Social & Cultural Engagement - Reinforcing,Written Communication - Integrated,Quantitative Problem Solving - Integrated,Analytical Reasoning & Scientific Inquiry - Integrated,Responsibility & Expression - Integrated,Social & Cultural Engagement - Integrated,BIOG: Upper Division Biology Electives,ENVS: Upper Division Science Electives,Human Health - Ecosystem Processes Cluster,Human Health - Policy Cluster,COMM: Communication Methods Cluster,COMM: Diversity Cluster,SCWL: Sustainable Practitioner Cluster,SCWL: Society and Natural World Foundation Course,RECR: Recreation Management Cluster,RECR: Diversity Cluster,RECR: Experiential Cluster,RECR: Natural World Cluster,PACM: Human Dimension Cluster,PACM: Natural World Cluster,NRCM: Cultural Perspective Cluster,NRCM: Ecosystem Management Cluster,NRCM: Negotiation/Planning Cluster,NRCM: Organisms/Habitats Cluster,NRCM: Practitioner Skills Cluster,NRCM: Society and Natural World Foundation Course,HRTM: Customer Relations Cluster,HRTM: Diversity Cluster,HRTM: Management Cluster,FWSW - Wildlife Concentration: Zoology Elective,FWSW - Wildlife Concentration: Botany Elective,FWSW - Wildlife Concentration: Ecology Elective,FWSW - Wildlife Concentration: Physical Science Elective,"FWSW - Wildlife Concentration: Policy, Admin & Law Elective","FWSW - Wildlife Concentration: Policy, Admin & Law Elective",FWSW - Fisheries Concentration: Biological Science Elective,FWSW - Fisheries Concentration: Human Dimension Elective,Forestry - Biology Concentration: Biology Cluster,FSMT: Customer Relations Cluster,FSMT: Diversity Cluster,FSMT: Management Cluster,ENST: Environment & Human Expression Cluster,ENST: Environment & Society Cluster,ENST: Environment & Science Cluster,ENST: Practitioner Skills Cluster,ENST: Society and Natural World Foundation Course,ECOR: Human System Cluster,BASM: Management Cluster,FORT: Forest Technology Cluster,Integrative Studies: BASM Program Options,Integrative Studies: BASM Select One Program Options,Integrative Studies: BIOG Program Options,Integrative Studies: COMM Program Options,Integrative Studies: CASM Program Options,Integrative Studies: CASM Select One Program Options,Integrative Studies: ECOR Program Options,Integrative Studies: EBSB Program Options,Integrative Studies: ENVS Program Options,Integrative Studies: ENST Program Options,Integrative Studies: FOR Program Options,Integrative Studies: FW Program Options,Integrative Studies: FSMT Program Options,Integrative Studies: HRTM Program Options,Integrative Studies: HRTM Select One Program Options,Integrative Studies: MNGT Program Options,Integrative Studies: NRCM Program Options,Integrative Studies: PACM Program Options,Integrative Studies: PSYCH Program Options,Integrative Studies: RECR Program Options,Integrative Studies: SCWL Program Options,Integrative Studies: AALM Program Options,Integrative Studies: SURV Program Options,Language Sequence - Spanish,Language Sequence - French,Language Sequence - Italian,SCM: Sustainable Communities Minor Cluster,SCM: Sustainable Communities Minor - Practitioner Courses,Capstone Courses,Management Core,Entrepreneurship Core,Sports and Event Management Core,Biology Minor UD Electives,Botany Minor Electives,Craft Beer Studies Electives,Entrepreneurial Business Minor Electives,Environmental Communications Minor Electives,Environmental Studies Minor Communication Methods Electives,Environmental Studies Minor Electives,Forestry Minor Electives,Geographic Information Systems Minor Electives,Maple Production & Products Electives,Sustainable Communities Minor Electives,Sustainable Communities Minor Practitioner Electives
Financial Accounting,ACC101.LEC.2010,X,,,,,,,,X,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,X,,,,X,,X,,,,X,,X,,,,,X,,X,X,,,,,,,,,,,,,,,,,,,,,,,,,,,,
Managerial Accounting,ACC102.LEC.2010,X,,,,,,,,X,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,X,,,,,,,,,,,,X,,,,,,,,X,,,,,,,,,,,,,,,,,,,,X,,,,,,,,
Small Business Accounting,ACC301.LEC.2011,X,,,,,,,,,,,,,,,,,,,,,,,,,X,,,,,,,,,,,,,,,,,,,,,,,,,,X,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,X,X,,,,,X,,,,,,,,
Analytical Reasoning Foundational,AR100.2011,X,X,,,X,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,

Description needs to be less than 64 characters
Abreviation needs be less than 17
'''

import pandas as pd
import numpy as np
import datetime
from pathlib import Path


# EDIT THESE VARIABLES APPROPRIATELY

today = datetime.datetime.now().strftime('%Y%m%d')

path = Path(r'\\psc-data\E\Applications\Starfish\Files\workingfiles\requirement_course_sets')

# catalog_year uses catalog_year in the naming convention of the files to keep them segregated
catalog_year = '2020'

# import_csv_file - this file is produced by exporting the appropriate csv from genedcourses.xlsx
import_csv_file = path / ('requirements_course_sets_input.csv')

# output_csv_file - this file is what is produced to be provided to starfish
#output_csv_file = path / (today + '_' + catalog_year + '_requirement_course_sets.txt')
output_csv_file = path / (catalog_year + '_requirement_course_sets.txt')


import csv
with open (import_csv_file) as csvfile:
    freader = csv.reader(csvfile, delimiter=',', quotechar = '"')
    courses = []
    count = 0
    lines = []
    abbreviation_names = []
    title_names = []

    for row in freader:
        if count == 0:
            for x in range(0, len(row)):
                abbreviation_names.append(row[x])
            #print(abbreviation_names)
        #populate set titles
        if count == 1:
            for x in range(0, len(row)):
                title_names.append(row[x])
        else:
            #CourseID = row[1].replace(' ','')
            CourseID = row[1]
            for x in range(0, len(row)):
                if row[x] == 'X':
                    lines.append([CourseID,catalog_year,abbreviation_names[x],title_names[x]])
                    
        
        count += 1
headers = ['course_integration_id','catalog_year','set_abbreviation','set_title']
df = pd.DataFrame.from_records(lines, columns = headers)
#print df

sorteddf = df.sort_values(by=['set_abbreviation','set_title'])
#print(sorteddf)

sorteddf.to_csv(output_csv_file, sep=',', index = False)

#OutputFile = open(output_csv_file, 'wb')
#with OutputFile:
#    writer = csv.writer(OutputFile)
#    writer.writerows(lines)
         
print("Writing complete")
