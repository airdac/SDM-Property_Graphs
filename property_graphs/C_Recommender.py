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
        # Create the database community
        execute_print(driver, '''
            MERGE(r: Research_community {name: 'database'})
                    ''')
        # Relate the database keywords to the database community
        execute_print(driver, '''
            MATCH(k: Keyword)
            WHERE k.tag IN['data management', 'indexing', 'data modeling', 'big data', 'data processing', 'data storage', 'data querying']
            MATCH(r: Research_community)
            MERGE(k)-[:From_r] -> (r)
                    ''')
        # Find the database community's conferences and journals and assert the relation
        execute_print(driver, '''
            // Match conferences/journals with papers
            MATCH (cj:Conference|Journal)<-[:From_c|From_j]-(:Edition|Volume)<-[:Published_in_e|Published_in_v]-(p:Paper)
            WITH cj, COUNT(p) AS n_papers
            // Match conferences/journals with database papers
            MATCH (cj)<-[:From_c|From_j]-(:Edition|Volume)<-[:Published_in_e|Published_in_v]-(db_p:Paper)<-[:From_p]-(:Keyword)-[:From_r]->(r:Research_community {name: 'database'})
            WITH r, cj, n_papers, COUNT(DISTINCT db_p) AS n_db_papers
            WITH r, cj, 1.0 * n_db_papers/ n_papers AS percentage_db_papers
            WHERE percentage_db_papers >= 0.9
            MERGE (cj)-[:Related_to]->(r)
                      ''')
        # Find the top-100 papers of the conferences and journals of the database community and assert the relation
        execute_print(driver, '''
            MATCH (r:Research_community {name: 'database'})<-[:Related_to]-(:Conference|Journal)<-[:From_c|From_j]-(:Edition|Volume)<-[:Published_in_e|Published_in_v]-(cited_p:Paper)<-[citation:Cites]-(citing_p:Paper)-[:Published_in_e|Published_in_v]->(:Edition|Volume)-[:From_c|From_j]->(:Conference|Journal)-[:Related_to]->(r)
            WITH r, cited_p, COUNT(citation) as n_citations
            ORDER BY n_citations DESC
            WITH r, COLLECT(cited_p)[..100] AS TOP_CITED_PAPERS
            UNWIND TOP_CITED_PAPERS AS paper
            MERGE (paper)-[:Is_top_paper_of]->(r)
                        ''')
        # Find good matches to review database papers
        execute_print(driver, '''
            MATCH(a: Author)-[:Writes | Co_writes] -> (: Paper)-[:Is_top_paper_of] -> (r: Research_community {name: 'database'})
            MERGE(a)-[:Good_match_to_review] -> (r)
                                ''')
        # Find database gurus
        execute_print(driver, '''
            MATCH (a:Author)-[:Writes|Co_writes]->(p:Paper)-[:Is_top_paper_of]->(r:Research_community {name: 'database'})
            WITH r, a, COUNT(p) AS n_papers
            WHERE n_papers >= 2
            MERGE (a)-[:Is_a_guru_of]->(r)
                                ''')

    except Exception as e:
        print('\nException raised:')
        print(e)
