    # "has" edge
    driver.execute_query("""
    LOAD CSV WITH HEADERS FROM 'file:///has_edge.csv' AS row
     
    MATCH (journal:Journal {name: row.journal}), (vol:Volume {id: row.volume})
    MERGE (journal)-[r:HAS]->(vol)
                         """) 

    # Reviews edge
    driver.execute_query("""
    LOAD CSV WITH HEADERS FROM 'file:///reviews_edge.csv' AS row 
    WITH row, split(row.reviewers, ",") AS reviwers
    UNWIND reviwers as reviwers
    MATCH (author:Author {full_names: reviewers}), (paper:Paper {title: row.paper})
    MERGE (author)-[:REVIEWS {date: row.date}]->(paper) 
                         """) 
    
    # "Cites" edge
    driver.execute_query("""
    LOAD CSV WITH HEADERS FROM 'file:///cited_in.csv' AS row 
    UNWIND row.cites AS cites
    MATCH (source:Paper {title: row.paper})
    MATCH (target:Paper {title: cites})
    MERGE (source)-[r:CITES]->(target)
                         """)