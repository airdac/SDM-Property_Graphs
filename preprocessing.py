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
import yake

# Data path
# CHANGE IF NEEDED
# TEMP = 'C:\\Users\\Airdac\\Documents\\Uni\\UPC\\2nSemestre\\SDM\\Lab Property Graphs\\data&program\\dblp-to-csv-master'
TEMP = 'D:\\Documents\\Data Science\\Semantic Data Management\\Lab1\\dblp-to-csv-master'
TEMP += '\\%s.csv'

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
    # Code made by Erfan (2019) extracted from
    # https://stackoverflow.com/questions/57617456/split-pandas-dataframe-column-list-values-to-duplicate-rows
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
    uptodate_ee = [x if x is np.nan else x.split('|')[-1] for x in df.ee]
    df.assign(ee = uptodate_ee)

    return df

def keywords(id, title, full_dict = {}, valid_dict = {}, valid_article = set(), numOfKeywords = 4, max_ngram_size = 2, deduplication_threshold = 0.9):
    """
    Functionality: Detect keywords from titles of articles using natural a language processing library.
    Each library has 4 keywords (numOfKeywords) that have a maximum len of 2 words (max_ngram_size). We
    are very flexible on the keywords (deduplication_threshold is hight) so they may contain duplicated words (ex:
    'Python' and 'Python System' may be two different keywords).

    Input: Variables from an article (id, title) and dictionaries that store all keywords and valid ones
        (those that are found in more than 3 articles)
    Output: Updated dictionaries after considering a new article
    """

    # Code adapted from Manmohan Singh (2021)
    # https://towardsdatascience.com/keyword-extraction-process-in-python-with-natural-language-processing-nlp-d769a9069d5c
    custom_kw_extractor = yake.KeywordExtractor(n=max_ngram_size, dedupLim=deduplication_threshold, top=numOfKeywords, features=None)
    keywords = custom_kw_extractor.extract_keywords(title)

    # Check if the keyword already exist, otherwise add it to the dict
    for key in keywords:
        full_dict.setdefault(key[0], set())
        full_dict[key[0]].add(id)

        # Return keys that have more than 2 entries
        if len(dict[key[0]]) > 2:
            valid_dict[key[0]] = full_dict[key[0]]
            valid_article.add(id)
            
    return dict, valid_dict, valid_article

if __name__ == '__main__':
    # Select articles from metadata to import from each file
    col_article = ['article', 'ee', 'author', 'author-orcid'
                   , 'journal', 'month', 'year', 'title', 'volume']
    col_inproc = ['inproceedings', 'ee', 'author', 'author-orcid'
                  , 'crossref', 'month', 'year', 'title', 'volume']
    col_proc = ['proceedings', 'booktitle', 'title', 'key', 'year']


    article_raw = feature_extraction(TEMP % 'dblp_article'
                                     , TEMP % 'dblp_article_header'
                                     , 5000
                                     , col_article)
    inproc_raw = feature_extraction(TEMP % 'dblp_inproceedings'
                                    , TEMP % 'dblp_inproceedings_header'
                                    , 10000
                                    , col_inproc)
    proc_raw = feature_extraction(TEMP % 'dblp_proceedings'
                                  , TEMP % 'dblp_proceedings_header'
                                  , 10000
                                  , col_proc)


    # Select from inproc papers that are from a conference
    # Select from proc conferences
    # They are identified by the the fact that their key starts with "conf/"
    proc_index = proc_raw['key'].str.contains('conf/', regex=False)
    inproc_index = inproc_raw['crossref'].str.contains('conf/', regex=False)

    proc_raw = proc_raw.loc[proc_index]
    #inproc_raw = inproc_raw.loc[inproc_index] # THIS MAKES AN ERROR

    # Join inproc and proc dataframes: "cross-ref" in inproc is "key" in proc
    # The resulting dataframe has few rows
    proc_raw.rename(columns = {'key':'crossref'
                            , 'title': 'con_title'
                            , 'year' : 'con_year'}, inplace = True) 
    conference_raw = pd.merge(inproc_raw, proc_raw, on = 'crossref')

    # Identify keywords for all article and select the one
    # that are common in at least 3 articles.
    dict = {}
    valid_dict = {}
    valid_article = set([])
    
    for i in range(len(article_raw)):
        dict, valid_dict, valid_article = keywords(i, article_raw.title.iloc[i], dict, valid_dict, valid_article)

    print('There are', len(valid_dict), 'keywords that are in 3 articles or more')

    keywords = pd.DataFrame(list(valid_dict.items()), columns = ['Keyword', 'Article_id'])

    # Generate keywords for articles that do not have one
    gen_articles = set(range(0,5000)) - valid_article
    print(gen_articles)

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

"""
keywords = dict()
keywords.keys = []
#keywords = {physics: [], maths: [], ...}
for keyword in keywords.keys:
    for title in article.titles.append(conference.title):   # CAREFULL: PD.DATAFRAME APPEND
        if keyword in title:
            keywords[keyword].append[title]
"""