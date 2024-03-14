"""
Info: The program read a sample of files: article, inproceedings and proceedings
-"proceedings.csv" contains data about a conference or workshop such as title, year, place. 
-"inproceedings.csv" has data from papers and authors that published in conferences or workshops. 
-"article.csv" has info about papers and authors published in journals. Basic infomation about 
journals is also extracted from here.
"""
import pandas as pd
import numpy as np

def feature_extraction(name_datacsv, name_headers, n_sample, col_names):
    """
    Input: 
        -name_datacsv: Name of the csv, without headers. First column must contain an ID.
        -name_headers: Row with all headers and types (header_name:type) of data
        -n_sample: number of rows to retrieve (it starts at 0)
        -col_names: name of the headers that you want to inport
    Output: A Dataframe with size (n_sample, col_names)
    """
    headers = pd.read_csv(name_headers
                                , delimiter=';'
                                , header=None)

    headers_list = [s.split(':') for s in list(headers.iloc[0])]
    headers_col_names = [col for col, _ in headers_list]

    selected_data_raw = pd.read_csv(name_datacsv
                        , delimiter = ';'
                        , nrows = n_sample
                        , names = headers_col_names
                        , index_col = headers_col_names[0]
                        , usecols = col_names
                        , header = None)
    
    # Remove rows that do not have an ID
    
    return selected_data_raw

# Select articles from metadata to import from each file
col_article = ['article', 'ee', 'author', 'author-orcid', 'journal', 'month', 'year', 'title', 'volume']
col_inproc = ['inproceedings', 'ee', 'author', 'author-orcid', 'crossref', 'month', 'year', 'title', 'volume']
col_proc = ['proceedings', 'booktitle', 'title', 'key', 'year']

article_raw = feature_extraction('dblp_article.csv', 'dblp_article_header.csv', 5000, col_article)
inproc_raw = feature_extraction('dblp_inproceedings.csv', 'dblp_inproceedings_header.csv', 10000, col_inproc)
proc_raw = feature_extraction('dblp_proceedings.csv', 'dblp_proceedings_header.csv', 10000, col_proc)


# Select from inproc and proc only papers that are really from a conference
# they start the key with "conf/" instead of "journals/"
proc_index = proc_raw['key'].str.contains('conf/')
inproc_indexes = inproc_raw['crossref'].str.contains('conf/')

# IMPLEMENT: CONSIDER ONLY THE ROWS THAT SATISFY THE CONDITION

# Join inproc and proc dataframes: "cross-ref" in inproc is "key" in proc
# note that there will be few records as a consequence
# Also change names of duplicated labels to avoid confusion
proc_raw.rename(columns = {'key':'crossref'
                           , 'title': 'con_title'
                           , 'year' : 'con_year'}, inplace = True) 

conference_raw = pd.merge(inproc_raw, proc_raw, on = 'crossref')


# Preprocessing already implemented
# Ensure that each row of author and orchid only contains one name
# Split authors into their names and surnames
# Define a main author (is the first that appears for each article)

def authors_preprocessing(raw_data):
    """
    Input: Dataframe with 'author' and 'author-orcid' attributes
    Output: Dataframe with a single author and 'author-orcid' per row
    """

    # Change the name of a variable (author-orcid --> 'AuthorOrcid').
    raw_data.rename(columns = {'author-orcid':'AuthorOrcid'}, inplace = True) 

    # then, split authors and their AuthorOrcid.
    # Code made by Erfan (2019) extracted from
    # https://stackoverflow.com/questions/57617456/split-pandas-dataframe-column-list-values-to-duplicate-rows
    raw_data.author = raw_data['author'].str.split("|")
    raw_data['AuthorOrcid'] = raw_data['AuthorOrcid'].str.split("|") 

    raw_data = raw_data.assign(author = raw_data['author']).explode('author').reset_index(drop=True)
    raw_data = raw_data.assign(AuthorOrcid = raw_data['AuthorOrcid']).explode('AuthorOrcid').reset_index(drop=True)
    
    # Set the main author for each paper
    # code code made jezrael (2016) and Divakar(2017) extracted from
    # https://stackoverflow.com/questions/47115448/pandas-get-index-of-first-and-last-element-by-group
    # https://stackoverflow.com/questions/37725195/pandas-replace-values-based-on-index

    raw_data['is_main_author'] = False

    a = raw_data.title.values
    idx = np.concatenate(([0],np.flatnonzero(a[1:] != a[:-1])+1))
    raw_data.loc[idx, 'is_main_author'] = True

    # Split author into their names and surnames
    names_raw = raw_data['author'].str.split(" ", n = 1, expand=True)

    del raw_data['author']
    raw_data['names'] = names_raw[0]
    raw_data['surname'] = names_raw[1]

    return raw_data

article_raw = authors_preprocessing(article_raw)
conference_raw = authors_preprocessing(conference_raw)

# TO DO
# Generate data for citations (at least 3)
# Extract info of conference "title" (such as place or edition)
# Generate data for keywords (use a dictionary to have some keywords that are
# commonly used and assign them randomly?)
# change name of ee to DOI 
# fill NaN
# Generate edges (they all have an start-end)

