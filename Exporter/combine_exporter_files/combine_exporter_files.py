"""
Combines daily difference files from Starfish Exporter into one file.
"""
import pandas as pd
from pathlib import Path

exporter_path = Path(r'\\psc-data\E\Applications\Starfish\Files\exporter')
output_path = Path(r'\\psc-data\E\Data\exporter_data')

files = exporter_path.glob('*.csv')

file_types = set()
for f in files:
    ft = f.stem.split('-')[0]
    file_types.add(ft)

for ft in file_types:
    files = exporter_path.glob(ft + '*.csv')
    combined_df = pd.concat( [ pd.read_csv(f) for f in files ] )
    print(ft, combined_df.shape)
    #print(combined_df.columns)
    print(combined_df.info())
    combined_df = combined_df.drop_duplicates()
    print(ft, combined_df.shape)

    outfile = output_path / (ft + '.csv')
    print(outfile)
    combined_df.to_csv(outfile, index=False)
