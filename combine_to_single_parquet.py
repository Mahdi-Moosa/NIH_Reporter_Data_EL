import pandas as pd

df = pd.read_parquet(path='pubmed_data/data_lake/')

df = df.drop_duplicates()

df = df[df.PMID != 'NOT_FOUND;INVALID_JOURNAL'].copy(deep=True)

def convert_to_int(inp_str):
    '''Function to convert PMID string to int. Will return None, if int casting is not possible.'''
    try:
        x = int(inp_str)
    except ValueError as e:
        print(f'{inp_str} can not be converted to integer.')
        x = None
    return x

df['PMID_int'] = df.PMID.apply(convert_to_int)

df['PMID_int'] = df.PMID_int.astype('Int64') # Int64 in pandas can accept None values.

df = df[df.PMID_int.notna()]

df = df[['PMID_int', 'DOI']]

df.columns = ['PMID', 'DOI']

print(f'Final shape: {df.shape}')

df.to_parquet(path='pubmed_data/pubmed_to_doi_linktable.parquet')

print('Single parquet file saved.') # It might make sense to make partitioned parquet, if needed.