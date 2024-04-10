from neo4j import GraphDatabase, Result
from pathlib import Path
import pandas as pd


def df_transformer(result: Result) -> tuple[pd.DataFrame, str, int]:
    df = result.to_df()
    summary = result.consume()
    return df, summary


def execute_print(driver, query, db='neo4j'):
    records, summary, _ = driver.execute_query(query, database_=db,
                                               )
    # Print out query completion
    print("The query `{query}` returned {records_count} records in {time} ms.\n".format(
        query=summary.query, records_count=len(records),
        time=summary.result_available_after
    ))
    counters = summary.counters.__dict__
    if counters.pop('_contains_updates', None):
        print(f'Graph asserted with {counters}.\n\n')
    else:
        print('The graph has not been modified.\n\n')


def execute_print_save(driver, query, path, db='neo4j'):
    df, summary = driver.execute_query(query, database_=db, result_transformer_=df_transformer
                                       )
    df.to_csv(path, index=False)

    # Print out query completion
    print("The query `{query}` returned {records_count} records in {time} ms.\n".format(
        query=summary.query, records_count=len(df),
        time=summary.result_available_after
    ))
    counters = summary.counters.__dict__
    if counters.pop('_contains_updates', None):
        print(f'Graph asserted with {counters}.\n\n')
    else:
        print('The graph has not been modified.\n\n')


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
                'pageRank',
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
