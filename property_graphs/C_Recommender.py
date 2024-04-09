from neo4j import GraphDatabase
from pathlib import Path
from query_execution import *


URI = "bolt://localhost:7687"
AUTH = ("neo4j", "123456789")

OUT = Path(__file__).parent.parent.absolute()/'C_output'
OUT.mkdir(exist_ok=True)

# Run queries
with GraphDatabase.driver(URI, auth=AUTH) as driver:
    try:
        driver.verify_connectivity()
        execute_print(driver, '''
            MERGE(r: Research_community {name: 'database'})
                    ''')
        execute_print(driver, '''
            MATCH(k: Keyword)
            WHERE k.tag IN['data management', 'indexing', 'data modeling', 'big data', 'data processing', 'data storage', 'data querying']
            MATCH(r: Research_community)
            MERGE(k)-[:From_r] -> (r)
                    ''')
        execute_print(driver, '''
            // Match conferences/journals with papers
            MATCH (cj:Conference|Journal)<-[]-(:Edition|Volume)<-[]-(p:Paper)
            WITH cj, COUNT(p) AS n_papers
            // Match conferences/journals with database papers
            MATCH (cj)<-[]-(:Edition|Volume)<-[]-(db_p:Paper)<-[]-(:Keyword)-[]->(r:Research_community {name: 'database'})
            WITH r, cj, n_papers, COUNT(db_p) AS n_db_papers
            WITH r, cj, 1.0 * n_db_papers/ n_papers AS percentage_db_papers
            WHERE percentage_db_papers >= 0.9
            MERGE (cj)-[:Related_to]->(r)
                      ''')
    except Exception as e:
        print('\nException raised:')
        print(e)
