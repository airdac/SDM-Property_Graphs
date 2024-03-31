import pandas as pd
from pathlib import Path, PureWindowsPath

TEMP = PureWindowsPath('C:\\Users\\Airdac\\Documents\\Uni\\UPC\\2nSemestre\\SDM\\Lab Property Graphs\\data&program\\dblp-to-csv-master')
#TEMP = PureWindowsPath("D:\\Documents\\Data Science\\Semantic Data Management\\Lab1\\raw_csv")
TEMP = Path(TEMP)

headers = pd.read_csv(TEMP/'dblp_article_header.csv', delimiter=";", header=None)

headers_list = [s.split(":") for s in list(headers.iloc[0])]
headers_col_names = [col for col, _ in headers_list]


df = pd.read_csv(TEMP/'dblp_article.csv'
                 ,delimiter=";"
                 ,nrows=1000000,
                names=headers_col_names,
                index_col=headers_col_names[0]
                 ,usecols=["article","ee","author","journal","month","year","title","volume", ]
                )

print(df)