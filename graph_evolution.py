# TO DO
#   - How to add properties to edges
#   - How to get cypher query return in python

from neo4j import GraphDatabase

URI = "bolt://localhost:7687"
AUTH = ("neo4j", "123456789")

with GraphDatabase.driver(URI, auth=AUTH) as driver:
    driver.verify_connectivity()

    query_result = driver.execute_query('''
        MATCH ()
        RETURN COUNT(*)
    ''')

    print(query_result)