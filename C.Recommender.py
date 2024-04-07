from neo4j import GraphDatabase

URI = "bolt://localhost:7687"
AUTH = ("neo4j", "123456789")
db = 'neo4j'

# Run queries
with GraphDatabase.driver(URI, auth=AUTH) as driver:
    try:
        # 1. Find the top 3 most cited papers of each conference.
        records, summary, keys = driver.execute_query('''
            ''', database_=db
                                                      )
        for record in records:
            pass
            # print(record)
        print("The query `{query}` returned {records_count} records in {time} ms.".format(
            query=summary.query, records_count=len(records),
            time=summary.result_available_after
        ))
    except Exception as e:
        print('\nException raised:')
        print(e)
        