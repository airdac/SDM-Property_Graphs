import pandas as pd


def execute_print(driver, query, db='neo4j'):
    records, summary, _ = driver.execute_query(query, database_=db
                                               )

    # Print out query completion
    print("The query `{query}` returned {records_count} records in {time} ms.".format(
        query=summary.query, records_count=len(records),
        time=summary.result_available_after
    ))


def execute_print_save(driver, query, path, db='neo4j'):
    records, summary, _ = driver.execute_query(query, database_=db
                                               )

    # Save result as csv
    result = pd.DataFrame([record.data() for record in records])
    result.to_csv(path, index=False)

    # Print out query completion
    print("The query `{query}` returned {records_count} records in {time} ms.".format(
        query=summary.query, records_count=len(records),
        time=summary.result_available_after
    ))
