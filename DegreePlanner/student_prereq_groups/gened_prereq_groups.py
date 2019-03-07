#!/usr/bin/env python
# coding: utf-8

# In[1]:


"""
Data file for Starfish's DegreePlanner.
Creates pre-requisite groups based on geneds (SC-I, WC-F, etc.).
"""
import pandas as pd
from datetime import date
from pathlib import Path

output_path = Path(r'\\psc-data\E\Applications\Starfish\Files\workingfiles\student_prereq_groups')
fn_output = output_path / 'gened_student_prereq_groups.txt'


pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', -1)

#create a passing grades dataframe
passing_grades_df = ['A','A+','B','B+','C','C+','D','D+','P','TR']
gen_ed_codes = ['WC-F','QP-F','AR-F','RE-F','SC-F','WC-R','QP-R','AR-R','RE-R','SC-R','WC-I','QP-I','AR-I','RE-I','SC-I']


# In[2]:


#readd in the gened courses for special topics
st_399_gened_courses = pd.read_csv(Path(r'\\psc-data\E\Applications\Starfish\Files\workingfiles\student_prereq_groups\ST_GENED_COURSES.csv',index = False))
st_399_gened_courses.head(10).sort_values('course_section_integration_id')


# In[3]:


sections_df = pd.read_csv(Path(r'\\psc-data\E\Applications\Starfish\Files\test\sisdatafiles\sections.txt',index = False))
sections_df = sections_df[['course_section_id','course_integration_id']]
sections_df.head(10)
#,course_section_id,course_integration_id
#0,ACC 101.LEC.2011.Fall.01,ACC 101.LEC.2010


# In[4]:


outcomes_df = pd.read_csv(Path(r'\\psc-data\E\Applications\Starfish\Files\test\sisdatafiles\course_outcomes.txt',index = False))
#,user_integration_id,course_section_integration_id,midterm_grade,final_grade,credit_hours
#0,P000000006,HOS 310.LEC.2011.Summer.01,,A,3
outcomes_df.head(10)


# In[5]:


req_course_sets_df = pd.read_csv(Path(r'\\psc-data\E\Applications\Starfish\Files\test\sisdatafiles\2017_requirement_course_sets.txt',index = False))
#,course_integration_id,catalog_year,set_abbreviation,set_title
#0,AR100.2011,2017,AR-F,Analytical Reasoning & Scientific Inquiry - Foundation
req_course_sets_df.head(10)


# In[6]:


outcomes_section_pd = pd.merge(outcomes_df, sections_df, left_on='course_section_integration_id', right_on='course_section_id', how='inner')
outcomes_section_pd = outcomes_section_pd[['user_integration_id','course_section_integration_id','final_grade','course_integration_id']]
#outcomes_section_pd['course_integration_id'] = outcomes_section_pd['course_integration_id'].str.replace(' ','')
#,user_integration_id,course_section_integration_id,final_grade,course_integration_id
#0,P000000006,HOS 310.LEC.2011.Summer.01,A,HOS310.LEC.2010
outcomes_section_pd.sort_values('user_integration_id').head(10)


# In[7]:


outcomes_sections_sets_pd = pd.merge(outcomes_section_pd, req_course_sets_df, on='course_integration_id', how='inner')
outcomes_sections_sets_pd.head(10)


# In[8]:


outcomes_sections_sets_pd = outcomes_sections_sets_pd[['user_integration_id','set_abbreviation','final_grade']]
#,user_integration_id,set_abbreviation,final_grade
#0,P000000006,DEGREE_APPLICABL,A
outcomes_sections_sets_pd.head(10)


# In[9]:


#only keep passing grades and gened codes
result_pd = outcomes_sections_sets_pd.query("final_grade in @passing_grades_df").query("set_abbreviation in @gen_ed_codes")
result_pd.head(10)


# In[10]:


#takes the 399 special topics courses and prepared to merge them with the outcomes file
outcomes_sections_sets_pd_399 = pd.merge(outcomes_section_pd, st_399_gened_courses, on='course_section_integration_id', how='inner')
#outcomes_sections_sets_pd_399 = outcomes_sections_sets_pd_399[['user_integration_id','set_abbreviation','final_grade']]
outcomes_sections_sets_pd_399.head(1000)


# In[11]:


#in 399 dataframe, only keep passing grades and gened codes
result_pd_399 = outcomes_sections_sets_pd_399.query("final_grade in @passing_grades_df").query("set_abbreviation in @gen_ed_codes")
result_pd_399.head(10)


# In[12]:


#append the 399 and standard data frames
result_pd = result_pd.append(result_pd_399, ignore_index=True,sort=True)
result_pd.head(100)


# In[13]:


#remove the grade and ID column
result_pd = result_pd[['user_integration_id','set_abbreviation']]
result_pd.rename(columns={"user_integration_id": "student_integration_id","set_abbreviation": "prereq_group_identifier"}, inplace = True)
result_pd.head(100)






# In[14]:


#incorporate test scores for math scores over 199 to have met qp-f
#student_integration_id,test_id,numeric_score,date_taken
#P000024201,ACCUPLACER_ENGLISH,100,2010-07-09
#P000024201,ACCUPLACER_MATH,120,2010-07-09
#P000027130,ACCUPLACER_MATH,200,2015-05-20
#P000027147,ACCUPLACER_ENGLISH,100,2014-05-29
test_scores = pd.read_csv(Path(r'\\psc-data\E\Applications\Starfish\Files\prod\sisdatafiles\student_test_results.txt',index = False))
test_scores = test_scores[test_scores.test_id == 'ACCUPLACER_MATH']
test_scores = test_scores[test_scores.numeric_score > 199]
test_scores.head(10)






# In[15]:


#Remove all of the columns except the student integration id and add a new column for qp-f
test_scores = result_pd[['student_integration_id']]
test_scores['prereq_group_identifier'] = 'QP-F'
test_scores.head(10)


# In[16]:


#appending test_scores to results_pd
result_pd = pd.concat([result_pd,test_scores])
result_pd.head(10)


# In[17]:


#rename the headers to match the file requirements
result_pd.drop_duplicates(subset=['student_integration_id','prereq_group_identifier'], inplace=True)
result_pd.set_index(['student_integration_id','prereq_group_identifier'], inplace=True)
result_pd.head(10)


# In[18]:


result_pd.to_csv(Path(r'\\psc-data\E\Applications\Starfish\Files\workingfiles\student_prereq_groups\gened_student_prereq_groups.txt',index=False))

