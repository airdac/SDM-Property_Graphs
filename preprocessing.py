import pandas as pd

# Read a sample of dblp_article.csv
article_header = pd.read_csv('dblp_article_header.csv'
                             , delimiter=';'
                             , header=None)

article_header_list = [s.split(':') for s in list(article_header.iloc[0])]
article_col_names = [col for col, _ in article_header_list]

article = pd.read_csv('dblp_article.csv'
                      , delimiter=';'
                      , nrows=5000
                      , names=article_col_names
                      , index_col='article'
                      , usecols=['article', 'author', 'journal', 'month', 'year', 'title', 'volume']
                      , header=None)
print(article)

# Read a sample of dblp_proceedings.csv
proceedings_header = pd.read_csv('dblp_proceedings_header.csv'
                             , delimiter=';'
                             , header=None)

proceedings_header_list = [s.split(':') for s in list(proceedings_header.iloc[0])]
proceedings_col_names = [col for col, _ in proceedings_header_list]

proceedings = pd.read_csv('dblp_proceedings.csv'
                      , delimiter=';'
                      , nrows=5000
                      , names=proceedings_col_names
                      , index_col='proceedings'
                      , usecols=['proceedings']
                      , header=None)
print(proceedings)