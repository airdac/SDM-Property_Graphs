"""
Master's Degree in Data Science
Spring 2024
Semantic Data Management

Lab Assignment 1: Property Graphs
To be delivered on the 10th of April, 2024

Team members:
    Adrià Casanova Lloveras
    Víctor García Pizarro

Code functionality:
    The program reads a sample of files: article,
        inproceedings and proceedings
    -"proceedings.csv" contains data about a conference
        or workshop such as title, year, place. 
    -"inproceedings.csv" has data from papers and authors
        that published in conferences or workshops. 
    -"article.csv" has info about papers (and its authors)
        published in journals. Basic infomation about 
    journals is also extracted from this source.
"""

import pandas as pd
import numpy as np

def feature_extraction(name_datacsv, name_headers, n_sample, col_names):
    """
    Functionality: retrieve selected rows and columns from a .csv into a pd.DataFrame
    Input: 
        -name_datacsv: Name of the csv, without headers.
            First column must contain an ID.
        -name_headers: Row with all headers and types
            (header_name:type) of data
        -n_sample: number of rows to retrieve (it starts at 0)
        -col_names: name of the headers that you want to import
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
    
    return selected_data_raw


def authors_preprocessing(raw_data):
    """
    Functionality: splits authors of the same paper in different rows.
        It also splits the author column into name and surnames columns.
        Moreover, it sets the main author of each paper
        (the first one in the author column).
    Input: Dataframe with 'author' and 'author-orcid' attributes
    Output: Dataframe with a single 'author' and 'author-orcid' per row
    """

    # Rename author-orcid to AuthorOrcid
    raw_data.rename(columns = {'author-orcid':'AuthorOrcid'}
                    , inplace = True) 

    # Split author and AuthorOrcid
    raw_data.author = raw_data['author'].str.split("|")
    raw_data['AuthorOrcid'] = raw_data['AuthorOrcid'].str.split("|") 

    raw_data = raw_data.explode('author').reset_index(drop=True)
    raw_data = raw_data.explode('AuthorOrcid').reset_index(drop=True)
    
    # Set the main author for each paper
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

def ee_preprocessing(df):
    '''
    Functionality: For each paper, split ee by '|' and only
        keep the last value, since it corresponds to the most
        recent version of the paper
    Input: pd.DataFrame containing raw data referring to papers with column ee
    Output: input pd.DataFrame with clean column ee, according to the
        aboved described functionality
    '''
    
    actual_ee = [x if x is np.nan else x.split('|')[-1] for x in df.ee]
    df.assign(ee = actual_ee)

    return df

if __name__ == '__main__':

    # Select articles from metadata to import from each file
    col_article = ['article', 'ee', 'author', 'author-orcid'
                   , 'journal', 'month', 'year', 'title', 'volume']
    col_inproc = ['inproceedings', 'ee', 'author', 'author-orcid'
                  , 'crossref', 'month', 'year', 'title', 'volume']
    col_proc = ['proceedings', 'booktitle', 'title', 'key', 'year']

    article_raw = feature_extraction('dblp_article.csv'
                                     , 'dblp_article_header.csv'
                                     , 5000
                                     , col_article)
    inproc_raw = feature_extraction('dblp_inproceedings.csv'
                                    , 'dblp_inproceedings_header.csv'
                                    , 10000
                                    , col_inproc)
    proc_raw = feature_extraction('dblp_proceedings.csv'
                                  , 'dblp_proceedings_header.csv'
                                  , 10000
                                  , col_proc)


    # Select from inproc papers that are from a conference
    # Select from proc conferences
    # They are identified by the the fact that their key starts with "conf/"
    proc_index = proc_raw['key'].str.contains('conf/', regex=False)
    inproc_index = inproc_raw['crossref'].str.contains('conf/', regex=False)

    proc_raw = proc_raw.loc[proc_index]
    inproc_raw = inproc_raw.loc[inproc_index]

    # Join inproc and proc dataframes: "cross-ref" in inproc is "key" in proc
    # The resulting dataframe has few rows
    proc_raw.rename(columns = {'key':'crossref'
                            , 'title': 'con_title'
                            , 'year' : 'con_year'}, inplace = True) 
    conference_raw = pd.merge(inproc_raw, proc_raw, on = 'crossref')

    # Preprocess author and author-orcid columns
    article = authors_preprocessing(article_raw)
    conference = authors_preprocessing(conference_raw)

    article = ee_preprocessing(article)
    conference = ee_preprocessing(conference)

    # TO DO
    # Generate data for citations (at least 3)
    # Extract info of conference "title" (such as place or edition)
    # Generate data for keywords (use a dictionary to have
    # some keywords that are commonly used and assign them randomly?)
    # change name of ee to DOI 
    # fill NaN
    # Generate edges (they all have a start-end)
    # Create column to separate the number in the end of some surnames
    #   When it's none set it to 0001
    # Check that the surname column really contains surnames
    #   It could happend that an author has a compound name.
    #   Then, half of its name would be in the wrong column