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
from re import findall
import random
from datetime import date
from string import ascii_letters
from pathlib import Path, PureWindowsPath

random.seed(123)
np.random.seed(123)

# Data paths
# CHANGE IF NEEDED
# TEMP = PureWindowsPath('C:\\Users\\Airdac\\Documents\\Uni\\UPC\\2nSemestre\\SDM\\Lab Property Graphs\\data&program\\dblp-to-csv-master')
# TEMP = PureWindowsPath("D:\\Documents\\Data Science\\Semantic Data Management\\Lab1\\raw_csv")
TEMP = '/Users/airdac/Documents/DBLP'
TEMP = Path(TEMP)
# OUT = PureWindowsPath('D:\\Documents\\Data Science\\Semantic Data Management\\Lab1\\clean_csv')
# OUT = PureWindowsPath(
#    'C:\\Users\\Airdac\\.Neo4jDesktop\\relate-data\\dbmss\\dbms-f41df0b2-56a6-4b46-b1b6-b535211967a8\\import')
OUT = '/Users/airdac/Library/Application Support/Neo4j Desktop/Application/relate-data/dbmss/dbms-1f900707-234f-4bf5-85f5-df387949b63c/import'
OUT = Path(OUT)
# We only get papers from the last 25 years (current year not included) and we balance the data in each year
yearf = date.today().year
n_years = 25


def csv_full_read(name_datacsv, name_headers, col_names):
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
        names=headers_col_names,
        index_col=headers_col_names[0],
        usecols=col_names,
        header=None
    )

    return selected_data_raw


def csv_chunk_read(name_datacsv, name_headers, n_sample, col_names, chunksize):
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
        header=None, chunksize=chunksize
    )

    return selected_data_raw


def article_feature_extraction(name_datacsv, name_headers, col_names, chunksize=100, n_sample=10000, year0=yearf-n_years, yearf=yearf):
    '''
        Functionality: get data from years between year0 and yearf
    '''
    df = pd.DataFrame()
    years = list(range(year0, yearf))
    year_searched = years.pop()

    for chunk in csv_chunk_read(name_datacsv=TEMP / name_datacsv, name_headers=TEMP / name_headers, n_sample=n_sample, col_names=col_names, chunksize=chunksize
                                ):

        chunk = chunk[chunk.year == year_searched]
        df = pd.concat([df, chunk])
        if len(df[df.year == year_searched]) >= chunksize:
            if not years:
                break
            else:
                year_searched = years.pop()

    return df

# We only process papers from conferences we have data from. By filtering at read-time, we get to read more records in a reasonable time.


def inproc_feature_extraction(name_datacsv, name_headers, col_names, editions, chunksize=100, n_sample=100000, year0=yearf-n_years, yearf=yearf):
    '''
        Functionality: get papers from years between year0 and yearf and most conference editions
    '''
    df = pd.DataFrame()
    years = set(range(year0, yearf))

    for chunk in csv_chunk_read(name_datacsv=TEMP / name_datacsv, name_headers=TEMP / name_headers, n_sample=n_sample, col_names=col_names, chunksize=chunksize
                                ):

        chunk = chunk[chunk.year.isin(years) & chunk.crossref.isin(editions)]
        df = pd.concat([df, chunk])
        new_years = years.copy()
        for year in years:
            if len(df[df.year == year]) >= chunksize:
                new_years.remove(year)
        years = new_years
        if not years and df.crossref.unique().size >= 1000:
            break

    return df


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
        columns={"author": "author_id"}, inplace=True
    )

    # Split author
    # Code made by Erfan (2019) extracted from
    # https://stackoverflow.com/questions/57617456/split-pandas-dataframe-column-list-values-to-duplicate-rows
    raw_data.author_id = raw_data["author_id"].str.split("|")

    # Make a copy before preprocessing
    raw_data['author_id_raw'] = raw_data.author_id
    raw_data = raw_data.explode("author_id").reset_index(drop=True)

    # Set the main author for each paper

    raw_data["is_main_author"] = False

    a = raw_data.title.values
    idx = np.concatenate(([0], np.flatnonzero(a[1:] != a[:-1]) + 1))
    raw_data.loc[idx, "is_main_author"] = True

    # Split author into their names and surnames
    names_raw = raw_data["author_id"].str.split(" ", n=1, expand=True)

    raw_data["name"] = names_raw[0]
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
    uptodate_ee = [x if x is np.nan or type(
        x) == bool else x.split("|")[-1] for x in df.ee]
    df.assign(ee=uptodate_ee)
    df.rename(columns={"ee": "DOI"}, inplace=True)

    return df


def surname_preprocessing(df):
    """ """
    name_id = [
        surname if surname in [None, np.nan] else findall(r"\d+", surname)
        for surname in df.surname
    ]
    name_id = ["0001" if id in [[], None, np.nan] else id[0] for id in name_id]
    df["name_id"] = name_id

    surname = [
        (
            surname
            if surname in [None, np.nan, ""]
            else " ".join(findall(r"[^(\d|\s)]+", surname))
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
    Functionality: Detect keywords from titles of papers using a natural language processing library.
    Each paper has 4 keywords (numOfKeywords) that have a maximum length of 2 words (max_ngram_size). We
    are very flexible on the keywords (deduplication_threshold is high) so they may contain duplicated words
    (ex:'Python' and 'Python System' may be two different keywords).

    Input: Variables from a paper (id, title) and dictionaries that store all keywords and valid keywords
        (those that are found in more than 3 papers)
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
    # Some keywords are a single letter
    keywords = [keyword[0] for keyword in keywords]
    keywords = [keyword for keyword in keywords if len(keyword)>1] # A few keywords are just a single letter

    # Check if the keyword already exists, otherwise add it to the dict
    for key in keywords:
        full_dict.setdefault(key, set())
        full_dict[key].add(id)

        # Return keys that have more than 2 entries
        if len(full_dict[key]) == 3:
            valid_dict[key] = full_dict[key]
        elif len(full_dict[key]) > 3:
            valid_article.add(id)

    return full_dict, valid_dict, valid_article


def generate_keywords(dict, all_id, valid_id, n_keywords=200):
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


def add_keywords(keywords, population, min_values_len=10, max_values_len=30):
#    new_dict = {key: {} for key in keywords}
    new_dict = dict()
    for key in keywords:
        k = random.randrange(min_values_len, max_values_len)
        values = random.sample(population, k=k)
        new_dict[key] = set(values)
    return new_dict


def generate_reviews(paper_node, author_node, main_author, co_author):
    all_reviews, reviews = [None] * len(paper_node), set()

    for ind, paper in enumerate(paper_node.index):
        max_iteration = random.randrange(3, 10)
        paper_title = paper_node.title[paper]

        while len(reviews) < max_iteration:
            random_author = author_node.author_id.sample(1).iat[0]

            # Check that the author is not the main author or a co-writer of the paper
            if paper_title in main_author.keys()\
                    and random_author == main_author[paper_title]:
                continue
            if paper_title in co_author.keys()\
                    and random_author in co_author[paper_title]:
                continue
            reviews.add(random_author)

        # When all reviwers for a paper are generated store them and start again
        all_reviews[ind] = reviews
        reviews = set()

    return all_reviews


def node_creation(col_name, col_id, df1, df2=pd.DataFrame()):
    df1_subset = df1[col_name]

    if df2.empty:
        node = df1_subset.drop_duplicates(subset=col_id)
    else:
        df2_subset = df2[col_name]
        node_raw = pd.concat([df1_subset, df2_subset], ignore_index=True)
        node = node_raw.drop_duplicates(subset=col_id)
    return node


def generate_citations(papers, min_rel=10, max_rel=30):
    all_relations, relations = [None] * len(papers), set()

    for j, paper in enumerate(papers.index):
        max_iteration = random.randrange(min_rel, max_rel)
        paper_title = papers.title[paper]
        paper_year = papers.year[paper]
        # A paper can only cite previous papers
        possible_papers = papers.title[papers.year < paper_year]
        if possible_papers.empty:
            continue

        while len(relations) < max_iteration:
            random_paper = possible_papers.sample(1).iat[0]
            if random_paper == paper_title:
                continue
            relations.add(random_paper)

        all_relations[j] = relations
        relations = set()

    return all_relations


####################################################################################################
# Main
####################################################################################################

if __name__ == "__main__":
    ############################################################################################################
    # Read data
    # ´

    # Select articles from metadata to import from each file
    col_article = [
        "article",
        "ee",
        "author",
        "journal",
        "year",
        "title",
        "volume"
    ]
    col_inproc = [
        "inproceedings",
        "ee",
        "author",
        "crossref",
        "year",
        "title"
    ]
    col_proc = ["proceedings", "booktitle", "title", "key", "year"]

    proc_raw = csv_full_read(          # We read all proceedings because there are less than 10,000
        TEMP / "dblp_proceedings.csv", TEMP / "dblp_proceedings_header.csv", col_names=col_proc
    )
    inproc_raw = inproc_feature_extraction(
        TEMP / "dblp_inproceedings.csv", TEMP / "dblp_inproceedings_header.csv", col_inproc, editions=set(proc_raw.key.unique())
    )
    article_raw = article_feature_extraction(
        TEMP / "dblp_article.csv", TEMP / "dblp_article_header.csv", col_article
    )

    ############################################################################################################
    # Preprocessing
    ############################################################################################################

    # Select from inproc papers that are from a conference
    # Select from proc conferences
    # They are identified by the the fact that their key starts with "conf/"
    proc_index = proc_raw["key"].str.contains("conf/", regex=False)
    inproc_index = inproc_raw["crossref"].str.contains("conf/", regex=False)

    proc_raw = proc_raw.loc[proc_index]
    inproc_raw = inproc_raw.loc[inproc_index].fillna(False)

    # Join inproc and proc dataframes: "cross-ref" in inproc is "key" in proc
    proc_raw.rename(
        columns={"key": "crossref", "title": "edition_title",
                 "year": "edition_year"},
        inplace=True,
    )
    conference_raw = pd.merge(inproc_raw, proc_raw, on="crossref")

    # Preprocess author column
    article = authors_preprocessing(article_raw)
    conference = authors_preprocessing(conference_raw)
    conference.rename(columns={"booktitle": "con_shortname"}, inplace=True)

    article = ee_preprocessing(article)
    conference = ee_preprocessing(conference)

    article = surname_preprocessing(article)
    conference = surname_preprocessing(conference)

    ############################################################################################################
    # Creation of nodes
    ############################################################################################################

    # AUTHOR node
    author_node = node_creation(
        ["author_id", "name", "surname", "name_id"],
        "author_id",
        article,
        conference
    )
    author_node.dropna(inplace=True)
    author_node.to_csv(OUT/'author_node.csv', index=False)

    # PAPER node
    paper_node = node_creation(
        ["title", "DOI", "year"], "title", article, conference)
    paper_node.replace(False, "NaN", inplace=True)

    # Generate artificial abstracts
    random_abstract = []
    for i in range(len(paper_node)):
        # Extracted from https://stackoverflow.com/questions/18834636/random-word-generator-python
        letters = ascii_letters
        x = "".join(random.choices(letters, k=100))
        random_abstract.append(x)

    paper_node["abstract"] = random_abstract
    paper_node.dropna(inplace=True)
    paper_node.to_csv(OUT/'paper_node.csv', index=False)

    # KEYWORD node
    # Extract Keywords for each article
    all_keywords, valid_keywords = {}, {}
    valid_id = set()

    for i in range(len(paper_node)):
        all_keywords, valid_keywords, valid_id = extract_keywords(
            paper_node.index[i],
            paper_node.title.iat[i],
            all_keywords,
            valid_keywords,
            valid_id,
        )

    # Generate artificial keywords for the papers that are left
    valid_keywords = generate_keywords(
        valid_keywords, paper_node.index, valid_id)
    
    # Add the database community keywords to some papers randomly to be able to run the recommender on setion C
    db_keywords = ['data management', 'indexing', 'data modeling', 'big data', 'data processing',
               'data storage', 'data querying']
    valid_keywords.update(add_keywords(
        db_keywords, paper_node.index.to_list(), min_values_len=5, max_values_len=30))
    
    # Create keyword nodes data
    keywords_node = pd.DataFrame(
        valid_keywords.items(), columns=["keyword", "article"]
    )
    keywords_node = keywords_node.explode('article')
    paper_titles = paper_node['title'].copy()
    paper_titles.index = paper_titles.index.astype('object')
    keywords_node = keywords_node.merge(paper_titles, left_on='article', right_index=True)
    keywords_node.rename(columns={'title': 'paper'}, inplace=True)

    keywords_node[['keyword', 'paper']].to_csv(OUT/'keywords_node.csv', index=False)

    # JOURNAL node
    journal_node = node_creation(["journal"], "journal", article)

    editors = []
    for i in range(len(journal_node)):
        editors.append(author_node["author_id"].sample(1).iat[0])

    journal_node["editor"] = editors
    journal_node.dropna(inplace=True)
    journal_node.to_csv(OUT/'journal_node.csv', index=False)

    # VOLUME node
    volume_node = node_creation(["volume", "year"], "volume", article)
    volume_node.dropna(inplace=True)
    volume_node.to_csv(OUT/'volume_node.csv', index=False)

    # CONFERENCE node
    conference_node = node_creation(
        ["con_shortname"], "con_shortname", conference)
    conference_node.dropna(inplace=True)
    conference_node.to_csv(OUT/'conference_node.csv', index=False)

    # EDITION node
    edition_node = node_creation(
        ["edition_title", "edition_year"], "edition_title", conference
    )
    edition_node.dropna(inplace=True)
    edition_node.to_csv(OUT/'edition_node.csv', index=False)

    ############################################################################################################
    # Creation of edges
    ############################################################################################################
    # REVIEWS edge
    columns_used = ['title', 'author_id', 'is_main_author']
    all_data = pd.concat(
        [conference[columns_used], article[columns_used]], ignore_index=True)
    all_data = all_data[all_data.author_id.isin(author_node.author_id)
                        & all_data.title.isin(paper_node.title)]

    co_author, main_author = dict(), dict()
    for i in all_data.index:
        n_article = all_data.title[i]
        if all_data.is_main_author[i] == True:
            main_author[n_article] = all_data.author_id[i]

        else:
            co_author.setdefault(n_article, [])
            co_author[n_article].append(all_data.author_id[i])

    reviews = generate_reviews(paper_node, author_node, main_author, co_author)

    reviews_edge = pd.DataFrame(
        {'paper': paper_node.title, 'reviewers': reviews})
    reviews_edge = reviews_edge.explode('reviewers')
    reviews_edge.rename({'reviewers': 'reviewer'},
                        axis='columns', inplace=True)
    reviews_edge.to_csv(OUT/'reviews_edge.csv', index=False)

    # WRITES & CO_WRITES edges
    writes_edge = pd.DataFrame(main_author.items(), columns=[
                               "paper", "main_author"])
    co_writes_edge = pd.DataFrame(
        co_author.items(), columns=["paper", "co_authors"])
    co_writes_edge = co_writes_edge.explode('co_authors')
    co_writes_edge.rename({'co_authors': 'co_author'},
                          axis='columns', inplace=True)

    writes_edge.dropna(inplace=True)
    co_writes_edge.dropna(inplace=True)

    writes_edge.to_csv(OUT/'writes_edge.csv', index=False)
    co_writes_edge.to_csv(OUT/'co_writes_edge.csv', index=False)

    # CITES edge
    cited_in_edge = generate_citations(paper_node)
    cited_in_edge = pd.DataFrame(
        data={"paper": paper_node.title, "cites": cited_in_edge})
    cited_in_edge = cited_in_edge.explode('cites')
    cited_in_edge.to_csv(OUT/'cites_edge.csv', index=False)

    # FROM_C edge
    from_edge = pd.DataFrame(
        data={"conference": conference.con_shortname, "edition": conference.edition_title})
    from_edge.drop_duplicates(inplace=True)
    from_edge.to_csv(OUT/'from_c_edge.csv', index=False)

    # FROM_J edge
    has_edge = article[['journal', 'volume']].drop_duplicates()
    has_edge.to_csv(OUT/'from_j_edge.csv', index=False)

    # PUBLISED_IN_E edge
    is_from_edge = pd.DataFrame(
        data={"paper": conference.title, "edition": conference.edition_title})
    is_from_edge.drop_duplicates(inplace=True)
    is_from_edge.to_csv(OUT/'published_in_e_edge.csv', index=False)

    # PUBLISHED_IN_V edge
    published_in_edge = pd.DataFrame(
        data={"paper": article.title, "volume": article.volume})
    published_in_edge.drop_duplicates(inplace=True)
    published_in_edge.to_csv(OUT/'published_in_v_edge.csv', index=False)
