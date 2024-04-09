from neo4j import GraphDatabase
from pathlib import Path
from query_execution import *


URI = "bolt://localhost:7687"
AUTH = ("neo4j", "123456789")

OUT = Path(__file__).parent.parent.absolute()/'D_output'
OUT.mkdir(exist_ok=True)

# Run queries
with GraphDatabase.driver(URI, auth=AUTH) as driver:
    try:
        driver.verify_connectivity()

        ####################################################
        # PageRank algorithm on papers related by citations
        ####################################################

        # Create an in-memory graph if it does not exist
        driver.verify_connectivity()
        result = driver.execute_query('''
            RETURN gds.graph.exists('pageRank')
                  ''', database_='neo4j', result_transformer_=Result.single)

        result = result.data().values()
        graph_exists = next(iter(result))
        if not graph_exists:
            execute_print(driver, '''
                CALL gds.graph.project(
                    'pageRank'
                    , 'Paper'
                    , 'Cites'
                )
                                ''')
        # Run the algorithm
        execute_print_save(driver, '''
            CALL gds.pageRank.stream(
                'trainGraph',
                {nodeLabels: ['Paper'], relationshipTypes: ['Cites']}
            )
            YIELD nodeId, score
            RETURN gds.util.asNode(nodeId).title AS paper, score
            ORDER BY score DESC, paper
                           ''', OUT/'pageRank.csv')
        
        ################################################################
        # Node Similarity algorithm on authors who write/co-write papers
        ################################################################

        # Create an in-memory graph if it does not exist
        driver.verify_connectivity()
        result = driver.execute_query('''
            RETURN gds.graph.exists('nodeSimilarity')
                  ''', database_='neo4j', result_transformer_=Result.single)

        result = result.data().values()
        graph_exists = next(iter(result))
        if not graph_exists:
            execute_print(driver, '''
                CALL gds.graph.project(
                        'nodeSimilarity'
                        , ['Author', 'Paper']
                        , ['Writes', 'Co_writes'])
                                    ''')
        # Run the algorithm
        execute_print_save(driver, '''
            CALL gds.nodeSimilarity.stream('nodeSimilarity')
            YIELD node1, node2, similarity
            RETURN gds.util.asNode(node1).name_id AS author1, gds.util.asNode(node2).name_id AS author2, similarity
            ORDER BY similarity DESC, author1, author2
                           ''', OUT/'nodeSimilarity.csv')


    except Exception as e:
        print('\nException raised:')
        print(e)
