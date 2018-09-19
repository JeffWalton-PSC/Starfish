"""
Data file for Starfish's DegreePlanner.
Creates pre-requisite groups based on geneds (SC-I, WC-F, etc.).
"""
import pandas as pd
from datetime import date
from pathlib import Path
import os

prereq_output_path = Path(r'\\psc-data\E\Applications\Starfish\Files\workingfiles\student_prereq_groups\student_prereq_groups.txt')
prereq_input_files = []

def find_prereq_files():
    prereq_files_path = Path(r'\\psc-data\E\Applications\Starfish\Files\workingfiles\student_prereq_groups')
    import os
    for root, dirs, files in os.walk(prereq_files_path):
        for file in files:
            if file.endswith("student_prereq_groups.txt") and file != "student_prereq_groups.txt":
                prereq_input_files.append(os.path.join(root, file))

#find files that end with "student_prereq_groups.txt"
find_prereq_files()
#combine them
combined_csv = pd.concat( [ pd.read_csv(f) for f in prereq_input_files ] )
#output them to a new file
combined_csv.to_csv( prereq_output_path, index=False )