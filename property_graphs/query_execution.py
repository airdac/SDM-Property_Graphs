import pandas as pd
from neo4j import Result


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
