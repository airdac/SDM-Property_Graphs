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
import re
import random
import string
import os

random.seed(123)

# Data path
# CHANGE IF NEEDED
TEMP = 'C:\\Users\\Airdac\\Documents\\Uni\\UPC\\2nSemestre\\SDM\\Lab Property Graphs\\data&program\\dblp-to-csv-master'
#TEMP = "D:\\Documents\\Data Science\\Semantic Data Management\\Lab1\\raw_csv"
TEMP += "\\%s.csv"
#OUT = 'D:\\Documents\\Data Science\\Semantic Data Management\\Lab1\\clean_csv'
OUT = 'C:\\Users\\Airdac\\Documents\\Uni\\UPC\\2nSemestre\\SDM\\Lab Property Graphs\\data&program\\clean_csv'

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
    headers = pd.read_csv(name_headers, delimiter=";", header=None)

    headers_list = [s.split(":") for s in list(headers.iloc[0])]
    headers_col_names = [col for col, _ in headers_list]

    selected_data_raw = pd.read_csv(
        name_datacsv,
        delimiter=";",
        nrows=n_sample,
        names=headers_col_names,
        index_col=headers_col_names[0],
        usecols=col_names,
        header=None,
    )

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
    raw_data.rename(
        columns={"author-orcid": "AuthorOrcid", "author": "full_name"}, inplace=True
    )

    # Split author and AuthorOrcid
    # Code made by Erfan (2019) extracted from
    # https://stackoverflow.com/questions/57617456/split-pandas-dataframe-column-list-values-to-duplicate-rows
    raw_data.full_name = raw_data["full_name"].str.split("|")
    
    raw_data["AuthorOrcid"] = raw_data["AuthorOrcid"].str.split("|")

    raw_data['full_name_raw'] = raw_data.full_name # Make a copy before preprocessing
    raw_data = raw_data.explode("full_name").reset_index(drop=True)
    raw_data = raw_data.explode("AuthorOrcid").reset_index(drop=True)

    # Set the main author for each paper

    raw_data["is_main_author"] = False

    a = raw_data.title.values
    idx = np.concatenate(([0], np.flatnonzero(a[1:] != a[:-1]) + 1))
    raw_data.loc[idx, "is_main_author"] = True

    # Split author into their names and surnames
    names_raw = raw_data["full_name"].str.split(" ", n=1, expand=True)

    raw_data["names"] = names_raw[0]
    raw_data["surname"] = names_raw[1]

    return raw_data


def ee_preprocessing(df):
    """
    Functionality: For each paper, split ee by '|' and only
        keep the last value, since it corresponds to the most
        recent version of the paper
    Input: pd.DataFrame containing raw data referring to papers with column ee
    Output: input pd.DataFrame with clean column ee, according to the
        aboved described functionality
    """
    uptodate_ee = [x if x is np.nan else x.split("|")[-1] for x in df.ee]
    df.assign(ee=uptodate_ee)
    df.rename(columns={"ee": "DOI"}, inplace=True)

    return df


def surname_preprocessing(df):
    """ """
    name_id = [
        surname if surname in [None, np.nan] else re.findall(r'\d+', surname)
        for surname in df.surname
    ]
    name_id = ["0001" if id in [[], None, np.nan] else id[0] for id in name_id]
    df["name_id"] = name_id

    surname = [
        (
            surname
            if surname in [None, np.nan, ""]
            else " ".join(re.findall(r'[^(\d|\s)]+', surname))
        )
        for surname in df.surname
    ]
    df["surname"] = surname

    return df


def extract_keywords(
    id,
    title,
    full_dict={},
    valid_dict={},
    valid_article=set(),
    numOfKeywords=4,
    max_ngram_size=2,
    deduplication_threshold=0.9,
):
    """
    Functionality: Detect keywords from titles of papers using natural a language processing library.
    Each paper has 4 keywords (numOfKeywords) that have a maximum len of 2 words (max_ngram_size). We
    are very flexible on the keywords (deduplication_threshold is hight) so they may contain duplicated words
    (ex:'Python' and 'Python System' may be two different keywords).

    Input: Variables from a paper (id, title) and dictionaries that store all keywords and valid ones
        (those that are found in more than 3)
    Output: Updated dictionaries after considering a new paper
    """

    # Code adapted from Manmohan Singh (2021)
    # https://towardsdatascience.com/keyword-extraction-process-in-python-with-natural-language-processing-nlp-d769a9069d5c
    custom_kw_extractor = yake.KeywordExtractor(
        n=max_ngram_size,
        dedupLim=deduplication_threshold,
        top=numOfKeywords,
        features=None,
    )
    keywords = custom_kw_extractor.extract_keywords(title)

    # Check if the keyword already exist, otherwise add it to the dict
    for key in keywords:
        full_dict.setdefault(key[0], set())
        full_dict[key[0]].add(id)

        # Return keys that have more than 2 entries
        if len(full_dict[key[0]]) > 2:
            valid_dict[key[0]] = full_dict[key[0]]
            valid_article.add(id)

    return full_dict, valid_dict, valid_article


def generate_keywords(dict, all_id, valid_id, n_keywords=20):
    """
    Functionality: Generate a number of keywords and apply them
        to papers that do not have one
    Input: ID of all papers and the ones that have an id (valid_id)
    Output: Dictionary with keywords as keys and id's as values
    """
    resting_id = set(all_id) - valid_id

    while len(resting_id) != 0:
        id = resting_id.pop()
        val = id % n_keywords
        random_tag = "Random_tag_" + str(val)

        dict.setdefault(random_tag, set())
        dict[random_tag].add(id)
    return dict

def generate_reviews(paper_node, author_node, main_author, co_author):
    all_reviews, reviews = [None] * len(paper_node), []

    for j in range(0, len(paper_node)):
        max_iteration = random.randrange(3,10)
        n_iteration = 0

        while n_iteration != max_iteration:
            random_author = random.choice(author_node.full_name.to_list())
            paper_title = paper_node.title.to_list()[j]

            # Check that the author is not the main author or co-writer of the paper.
            try:
                if random_author not in main_author[paper_title] + co_author[paper_title]:
                    reviews.append(random_author)
                    n_iteration += 1
            except KeyError:
                reviews.append(random_author)
                n_iteration += 1
            except TypeError:
                pass

        # When all reviwers for a paper are generated store them and start again  
        all_reviews[j] = reviews
        reviews = []
                           
    return all_reviews

def node_creation(col_name, col_id, df1, df2=pd.DataFrame()):
    df1_subset = df1[col_name].copy()

    if df2.empty:
        node = df1_subset.drop_duplicates(subset=col_id)
    else:
        df2_subset = df2[col_name].copy()
        node_raw = pd.concat([df1_subset, df2_subset], ignore_index=True)
        node = node_raw.drop_duplicates(subset=col_id)
    return node

def relation_generation(df1, df2 = pd.DataFrame(), min_rel = 1, max_rel = 5):
    all_relations, relations = [None] * len(df1), []      
    df1 = df1.to_list()

    for j in range(0, len(df1)):
        max_iteration = random.randrange(min_rel,max_rel)
        n_iteration = 0

        while n_iteration != max_iteration:
            if df2.empty == True:
                random_element = random.choice(df1)

                if df1[j] != random_element:
                    relations.append(random_element)
                    n_iteration += 1 
            else:
                random_element = random.choice(df2.to_list())
                relations.append(random_element)
                n_iteration += 1 

        all_relations[j] = relations
        relations = []
                            
    return all_relations

if __name__ == "__main__":
    # Select articles from metadata to import from each file
    col_article = [
        "article",
        "ee",
        "author",
        "author-orcid",
        "journal",
        "month",
        "year",
        "title",
        "volume",
    ]
    col_inproc = [
        "inproceedings",
        "ee",
        "author",
        "author-orcid",
        "crossref",
        "month",
        "year",
        "title",
        "volume",
    ]
    col_proc = ["proceedings", "booktitle", "title", "key", "year"]

    article_raw = feature_extraction(
        TEMP % "dblp_article", TEMP % "dblp_article_header", 2500, col_article
    )
    inproc_raw = feature_extraction(
        TEMP % "dblp_inproceedings",
        TEMP % "dblp_inproceedings_header",
        5000,
        col_inproc,
    )
    proc_raw = feature_extraction(
        TEMP % "dblp_proceedings", TEMP % "dblp_proceedings_header", 5000, col_proc
    )

    # Select from inproc papers that are from a conference
    # Select from proc conferences
    # They are identified by the the fact that their key starts with "conf/"
    proc_index = proc_raw["key"].str.contains("conf/", regex=False)
    inproc_index = inproc_raw["crossref"].str.contains("conf/", regex=False)

    proc_raw = proc_raw.loc[proc_index]
    inproc_raw = inproc_raw.loc[inproc_index].fillna(False)

    # Join inproc and proc dataframes: "cross-ref" in inproc is "key" in proc
    # The resulting dataframe has few rows
    proc_raw.rename(
        columns={"key": "crossref", "title": "edition_title", "year": "edition_year"},
        inplace=True,
    )
    conference_raw = pd.merge(inproc_raw, proc_raw, on="crossref")

    # Preprocess author and author-orcid columns
    article = authors_preprocessing(article_raw)
    conference = authors_preprocessing(conference_raw)
    conference.rename(columns={"booktitle": "con_shortname"}, inplace=True)

    article = ee_preprocessing(article)
    conference = ee_preprocessing(conference)

    article = surname_preprocessing(article)
    conference = surname_preprocessing(conference)

    ### Creation of nodes
    ## AUTHOR node
    author_node = node_creation(
        ["full_name", "names", "surname", "AuthorOrcid", "name_id"],
        "full_name",
        article,
        conference,
    )
    author_node = author_node.dropna()
    author_node.to_csv(os.path.join(OUT, r'author_node.csv'), index=False)

    ## Paper node
    paper_node = node_creation(["title", "DOI", "month"], "title", article, conference)
    paper_node.replace(False, "NaN", inplace=True)  # Change False to Nan
    random_abstract = []

    for i in range(len(paper_node)):
        # Extracted from https://stackoverflow.com/questions/18834636/random-word-generator-python
        letters = string.ascii_letters
        x = "".join(random.sample(letters, 10))
        random_abstract.append(x)

    paper_node["abstract"] = random_abstract
    paper_node = paper_node.dropna()
    paper_node.to_csv(os.path.join(OUT, r'paper_node.csv'), index=False)

    ## KEYWORD node
    # Extract Keywords for each article
    all_keywords, valid_keywords = {}, {}
    valid_id = set([])

    for i in range(len(paper_node)):
        all_keywords, valid_keywords, valid_id = extract_keywords(
            paper_node.index[i],
            paper_node.title.iloc[i],
            all_keywords,
            valid_keywords,
            valid_id,
        )

    # Generate artificial keywords for papers that are left
    valid_keywords = generate_keywords(valid_keywords, paper_node.index, valid_id)

    keywords_node = pd.DataFrame(
        list(valid_keywords.items()), columns=["Keyword", "Article_id"]
    )
    keywords_node.to_csv(os.path.join(OUT, r'keywords_node.csv'), index = False)

    # JOURNAL node
    journal_node = node_creation(["journal"], "journal", article)
    
    editors = []
    for i in range(len(journal_node)):
        editors.append(random.choice(author_node["full_name"].tolist()))

    journal_node["editor"] = editors
    journal_node.dropna()
    journal_node.to_csv(os.path.join(OUT, r'journal_node.csv'), index = False)

    # VOLUME node
    volume_node = node_creation(["volume", "year"], "volume", article)
    volume_node.dropna()
    volume_node.to_csv(os.path.join(OUT, r'volume_node.csv'), index = False)

    # CONFERENCE node
    conference_node = node_creation(["con_shortname"], "con_shortname", conference)
    conference_node.dropna()
    conference_node.to_csv(os.path.join(OUT, r'conference_node.csv'), index = False)

    # EDITION node
    edition_node = node_creation(
        ["edition_title", "edition_year"], "edition_title", conference
    )
    edition_node.dropna()
    edition_node.to_csv(os.path.join(OUT, r'edition_node.csv'), index = False)

    ## EDGE creation
    # Reviews edge
    all_data = pd.concat([conference, article], ignore_index = True)

    co_author, main_author = dict(), dict()
    for i in range(len(all_data)):
        n_article = all_data.title[i]
        if all_data.is_main_author[i] == True:
            main_author[n_article] = all_data.full_name[i]
        else: 
            co_author[n_article] = all_data.full_name[i]

    reviews = generate_reviews(paper_node, author_node, main_author, co_author)
    
    reviews_edge = pd.DataFrame({'paper': paper_node.title, 'reviewers': reviews, 'date': paper_node.month})
    reviews_edge.to_csv(os.path.join(OUT, r'reviews_edge.csv'), index = False)

    # Writes & Co_writes edge
    writes_edge = pd.DataFrame(main_author.items(), columns=["paper", "main_author"])
    co_writes_edge = pd.DataFrame(co_author.items(), columns=["paper", "co_author"])
    
    writes_edge = writes_edge.dropna()
    co_writes_edge = co_writes_edge.dropna()

    writes_edge.to_csv(os.path.join(OUT, r'writes_edge.csv'), index = False)
    co_writes_edge.to_csv(os.path.join(OUT, r'co_writes_edge.csv'), index = False)
    
    # HAS edge
    relation = relation_generation(journal_node.journal, edition_node.edition_title)
    has_edge = pd.DataFrame(data={"journal": journal_node.journal, "volume": relation})
    has_edge.to_csv(os.path.join(OUT, r'has_edge.csv'), index = False)

    # "Is cited in" edge
    cited_in_edge = relation_generation(paper_node.title)
    cited_in_edge = pd.DataFrame(data={"paper": paper_node.title, "cites": cited_in_edge})
    cited_in_edge.to_csv(os.path.join(OUT, r'cited_in.csv'), index = False)

    # "Published in" edge
    published_in_edge = pd.DataFrame(data = {"paper": article.title, "volume": article.volume})
    published_in_edge.drop_duplicates(inplace=True)
    published_in_edge.to_csv(os.path.join(OUT, r'published_in_edge.csv'), index = False)

    # "Is from" edge
    is_from_edge = pd.DataFrame(data = {"paper": conference.title, "edition": conference.edition_title})
    is_from_edge.drop_duplicates(inplace=True)
    is_from_edge.to_csv(os.path.join(OUT, r'is_from_edge.csv'), index = False)

    # From edge 
    from_edge = pd.DataFrame(data = {"conference": conference.con_shortname, "edition": conference.edition_title})
    from_edge.drop_duplicates(inplace=True)
    from_edge.to_csv(os.path.join(OUT, r'from_edge.csv'), index = False)
