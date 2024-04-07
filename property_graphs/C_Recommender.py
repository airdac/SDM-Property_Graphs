from neo4j import GraphDatabase
from pathlib import Path
from query_execution import *


URI = "bolt://localhost:7687"
AUTH = ("neo4j", "123456789")
db = 'neo4j'

OUT = Path(__file__).parent.parent.absolute()/'C_output'
OUT.mkdir(exist_ok=True)

# Run queries
with GraphDatabase.driver(URI, auth=AUTH) as driver:
    try:
        execute_print(driver, '''
            MERGE(: Research_community {name: 'database'})
                    ''')
        execute_print(driver, '''
            MATCH(k: Keyword)
            WHERE k.tag IN['data management', 'indexing', 'data modeling', 'big data', 'data processing', 'data storage', 'data querying']
            MATCH(r: Research_community)
            MERGE(k)-[:From_r] -> (r)
                    ''')
    except Exception as e:
        print('\nException raised:')
        print(e)
