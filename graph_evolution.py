# TO DO
# - Add author's affilitations

from neo4j import GraphDatabase
import pandas as pd
from string import ascii_letters
import random
from pathlib import Path, PureWindowsPath

random.seed(123)

URI = "bolt://localhost:7687"
AUTH = ("neo4j", "123456789")
db = 'neo4j'

#OUT = PureWindowsPath('D:\\Documents\\Data Science\\Semantic Data Management\\Lab1\\clean_csv')
OUT = PureWindowsPath('C:\\Users\\Airdac\\.Neo4jDesktop\\relate-data\\dbmss\\dbms-f41df0b2-56a6-4b46-b1b6-b535211967a8\\import')
OUT = Path(OUT)
####################################
# Generate synthetic data
####################################

# Get number of reviews
with GraphDatabase.driver(URI, auth=AUTH) as driver:
    driver.verify_connectivity()

    record, _, _ = driver.execute_query('''
        MATCH ()-[:Reviews]->()
        RETURN COUNT(*)
        ''', database_=db
    )

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
reviews_edge = pd.read_csv(OUT/'reviews_edge.csv', usecols=['paper', 'reviewer'])

# Write review_param.csv
review_param = pd.DataFrame({'description': description, 'decision': decision})
review_param = pd.concat([review_param, reviews_edge], axis=1)
review_param.to_csv(OUT/'review_param.csv', index = False)

# Set review descriptions and decisions
with GraphDatabase.driver(URI, auth=AUTH) as driver:
    driver.verify_connectivity()

    records, summary, keys = driver.execute_query('''
        LOAD CSV WITH HEADERS FROM 'file:///review_param.csv' AS row
        MATCH (author:Author {name_id: row.reviewer})
        MATCH (paper:Paper {title: row.paper})
        MATCH (author)-[r:Reviews]->(paper)
        SET r.description = row.description
        SET r.decision = row.decision
        ''', database_=db
    )

    # Loop through results and do something with them
    for record in records:  
        print(record.data())  # obtain record as dict

    # Summary information  
    print("The query '{query}' returned {records_count} records in {time} ms.".format(
        query=summary.query, records_count=len(records),
        time=summary.result_available_after
    ))

# Add author's affiliation
author_ids = pd.read_csv(OUT/'author_node.csv', usecols=['author_id'], delimiter = ',')
universities = pd.read_csv('us-colleges-and-universities.csv', usecols=['NAME'], delimiter=';')
universities.rename({'NAME': 'affiliation'}, inplace=True)
affiliations = pd.concat([author_ids, universities], axis=1)
affiliations.to_csv(OUT/'affiliations.csv', index=False)

with GraphDatabase.driver(URI, auth=AUTH) as driver:
    driver.verify_connectivity()

    records, summary, keys = driver.execute_query('''
        LOAD CSV WITH HEADERS FROM 'file:///affiliations.csv' AS row
        MATCH (author:Author {name_id: row.author_id})
        SET author.affiliation = row.affiliation
        ''', database_=db
    )

    # Loop through results and do something with them
    for record in records:  
        print(record.data())  # obtain record as dict

    # Summary information  
    print("The query '{query}' returned {records_count} records in {time} ms.".format(
        query=summary.query, records_count=len(records),
        time=summary.result_available_after
    ))