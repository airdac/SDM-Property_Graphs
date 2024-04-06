from neo4j import GraphDatabase
import pandas as pd
import numpy.random
from string import ascii_letters
import random
from pathlib import Path, PureWindowsPath

random.seed(123)
numpy.random.seed(123)

URI = "bolt://localhost:7687"
AUTH = ("neo4j", "123456789")
db = 'neo4j'

# OUT = PureWindowsPath('D:\\Documents\\Data Science\\Semantic Data Management\\Lab1\\clean_csv')
# OUT = PureWindowsPath('C:\\Users\\Airdac\\.Neo4jDesktop\\relate-data\\dbmss\\dbms-f41df0b2-56a6-4b46-b1b6-b535211967a8\\import')
OUT = Path('/Users/airdac/Library/Application Support/\
           Neo4j Desktop/Application/relate-data/dbmss/\
           dbms-c8c0866b-a55b-44a2-9174-e2748cd0d05a/import')
OUT = Path(OUT)


######################################
# Set review descripion and acceptance
######################################

# Get number of reviews
with GraphDatabase.driver(URI, auth=AUTH) as driver:
    try:

        record, _, _ = driver.execute_query('''
            MATCH ()-[:Reviews]->()
            RETURN COUNT(*)
            ''', database_=db
                                            )
    except Exception as e:
        print(e)

result = record[0]
result = result.data().values()
n_reviews = next(iter(result))

# Generate synthetic review descriptions
description = []
for i in range(n_reviews):
    # Extracted from https://stackoverflow.com/questions/18834636/random-word-generator-python
    letters = ascii_letters
    x = "".join(random.choices(letters, k=100))
    description.append(x)

# Generate synthetic review decisions
decision = random.choices(['Accepted', 'Not Accepted'], k=n_reviews)

# Read reviews_edge.csv
# We match the properties to edges so that the cypher query be faster
reviews_edge = pd.read_csv(OUT/'reviews_edge.csv',
                           usecols=['paper', 'reviewer'])

# Write review_param.csv
review_param = pd.DataFrame({'description': description, 'decision': decision})
review_param = pd.concat([review_param, reviews_edge], axis=1)
review_param.to_csv(OUT/'review_param.csv', index=False)

# Set review descriptions and decisions
with GraphDatabase.driver(URI, auth=AUTH) as driver:
    try:
        driver.execute_query('''
            LOAD CSV WITH HEADERS FROM 'file:///review_param.csv' AS row
            MATCH (author:Author {name_id: row.reviewer})
            MATCH (paper:Paper {title: row.paper})
            MATCH (author)-[r:Reviews]->(paper)
            SET r.description = row.description
            SET r.decision = row.decision
            ''', database_=db
                             )
    except Exception as e:
        print(e)

########################################
# Set paper's acceptance
########################################

with GraphDatabase.driver(URI, auth=AUTH) as driver:
    try:
        driver.execute_query('''
            MATCH (paper:Paper)<-[r:Reviews]-()
            WITH paper, collect(r) AS reviews, count(*) as n_total
            WITH paper, n_total, size([review in reviews WHERE review.decision = 'Accepted' | review]) AS n_accepted
            SET paper.acceptance =
                CASE
                    WHEN n_accepted >= n_total - n_accepted THEN 'Yes'
                    ELSE 'No'
                END
            ''', database_=db
                             )
    except Exception as e:
        print(e)


########################################
# Set author's affiliation
########################################

author_ids = pd.read_csv(OUT/'author_node.csv',
                         usecols=['author_id'], delimiter=',')
universities = pd.read_csv(
    'us-colleges-and-universities.csv', usecols=['NAME'], delimiter=';')
universities.rename(columns={'NAME': 'affiliation'}, inplace=True)
author_ids['affiliation'] = universities.affiliation.sample(
    n=len(author_ids), replace=True).values

author_ids.to_csv(OUT/'affiliations.csv', index=False)

with GraphDatabase.driver(URI, auth=AUTH) as driver:
    try:
        driver.execute_query('''
            LOAD CSV WITH HEADERS FROM 'file:///affiliations.csv' AS row
            MATCH (author:Author {name_id: row.author_id})
            SET author.affiliation = row.affiliation
            ''', database_=db
                             )
    except Exception as e:
        print('\nException raised:')
        print(e)
