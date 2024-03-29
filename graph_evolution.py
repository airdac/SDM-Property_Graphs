# TO DO
#   ---- How to add properties to edges https://neo4j.com/docs/python-manual/current/query-simple/
#   ---- How to get cypher query return in python

from neo4j import GraphDatabase
import pandas as pd
from string import ascii_letters
import random
from os import path

random.seed(123)

URI = "bolt://localhost:7687"
AUTH = ("neo4j", "123456789")
db = 'neo4j'

#OUT = 'D:\\Documents\\Data Science\\Semantic Data Management\\Lab1\\clean_csv'
OUT = 'C:\\Users\\Airdac\\.Neo4jDesktop\\relate-data\\dbmss\\dbms-f41df0b2-56a6-4b46-b1b6-b535211967a8\\import'

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
result = list(result.data().values())
n_reviews = result[0]

# Generate synthetic review descriptions
description = []
for i in range(n_reviews):
    # Extracted from https://stackoverflow.com/questions/18834636/random-word-generator-python
    letters = ascii_letters
    x = "".join(random.choices(letters, k=100))
    description.append(x)

# Generate synthetic review decisions
decision = random.choices(['Accepted', 'Not Accepted'], k=n_reviews) 

review_param = pd.DataFrame({'description': description, 'decision': decision})
review_param.to_csv(path.join(OUT, r'review_param.csv'), index = False)

# Set review descriptions and decisions
with GraphDatabase.driver(URI, auth=AUTH) as driver:
    driver.verify_connectivity()

    records, summary, keys = driver.execute_query('''
        LOAD CSV WITH HEADERS FROM 'file:///review_param.csv' AS row
        MATCH ()-[r:Review]->()
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