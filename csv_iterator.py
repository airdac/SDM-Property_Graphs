# We want to have papers from 2021, 2022 and 2023 to answer the queries in section B
# So far we are loading the top 10,000 rows from the dblp_article.csv and dblp_inproc.csv files, which only have papers from 2023 and 2024
# Our goal now is, for each of the required years, to load at least one chunck of each file that contains papers from the required year

import pandas as pd
from pathlib import Path, PureWindowsPath

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
        header=None
        , chunksize=chunksize
    )

    return selected_data_raw

def feature_extraction(name_datacsv, name_headers, n_sample, col_names, years):
    df = pd.DataFrame()
    year_searched = years.pop()

    for chunk in csv_chunk_read(name_datacsv=TEMP / name_datacsv
                                , name_headers=TEMP / name_headers
                                , n_sample=n_sample
                                , col_names=col_names
                                , chunksize=1000
                                ):
        
        chunk = chunk[chunk.year == year_searched]
        df = pd.concat([df, chunk])
        if len(df[df.year == year_searched]) >= 1000:
            if not years:
                break
            else:
                year_searched = years.pop()

    return df

TEMP = PureWindowsPath('C:\\Users\\Airdac\\Documents\\Uni\\UPC\\2nSemestre\\SDM\\Lab Property Graphs\\data&program\\dblp-to-csv-master')
#TEMP = PureWindowsPath("D:\\Documents\\Data Science\\Semantic Data Management\\Lab1\\raw_csv")
TEMP = Path(TEMP)

col_article = [
            "article",
            "ee",
            "author",
            "journal",
            "month",
            "year",
            "title",
            "volume",
        ]
article = feature_extraction( "dblp_article.csv", "dblp_article_header.csv", 100000, col_article, list(range(2021, 2024)))

print(article)