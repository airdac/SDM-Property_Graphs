    # NODES load
    # Author
    driver.execute_query("""
    LOAD CSV WITH HEADERS FROM 'https://drive.google.com/uc?=download&id=1YcCDfBzjRADccMKxYtvhMKQ9jxZbELha' AS row 
    WITH row
    WHERE row.full_names IS NOT NULL
    MERGE (a:Author {full_names: row.full_name, names: coalesce(row.names, "NaN"), surname: coalesce(row.surname,"NaN"), 
                         OrcID: coalesce(row.AuthorOrcid, "NaN")}) 
                         """)

    # Papers
    driver.execute_query("""
    LOAD CSV WITH HEADERS FROM 'https://drive.google.com/uc?=download&id=16M5o0jfaXBhtPe2zXOmxxUwMVjQxCbNW' AS row 
    WITH row
    WHERE row.title IS NOT NULL
    MERGE (:Paper {title: row.title, DOI: coalesce(row.DOI, "NaN"), month: coalesce(row.month,"NaN"), 
                         abstract: coalesce(row.abstract, "NaN")}) 
                         """)

    # Journal
    # https://drive.google.com/file/d/1JOut31hOD8TPwEHgCpFiA7C6hn8G2mIq/view?usp=drive_link
    driver.execute_query("""
    LOAD CSV WITH HEADERS FROM 'https://drive.google.com/uc?=download&id=1JOut31hOD8TPwEHgCpFiA7C6hn8G2mIq' AS row 
    WITH row
    WHERE row.journal IS NOT NULL
    MERGE (:Journal {name: row.journal, main_editor: row.editor}) 
                         """)

    # Volume
    # https://drive.google.com/file/d/1p285s75bsZ17UkRPmXF-yRT6jhEAuctx/view?usp=drive_link
    driver.execute_query("""
    LOAD CSV WITH HEADERS FROM 'https://drive.google.com/uc?=download&id=1p285s75bsZ17UkRPmXF-yRT6jhEAuctx' AS row 
    WITH row
    WHERE row.volume IS NOT NULL
    MERGE (v:Volume {id: row.volume, year: toInteger(row.year)})
                         """)

    # Conference
    # https://drive.google.com/file/d/1WP8rC9LqnCcd1e-9SRFd1_SGcIMf5zL_/view?usp=sharing
    driver.execute_query("""
    LOAD CSV WITH HEADERS FROM 'https://drive.google.com/uc?=download&id=1WP8rC9LqnCcd1e-9SRFd1_SGcIMf5zL_' AS row 
    WITH row
    WHERE row.row.con_shortname IS NOT NULL
    MERGE (:Edition {title: row.con_shortname})
                         """)  

    # Edition
    # https://drive.google.com/file/d/1SmOAKqzGtTtvt1Bg4-5rFen2Beydha1_/view?usp=sharing
    driver.execute_query("""
    LOAD CSV WITH HEADERS FROM 'https://drive.google.com/uc?=download&id=1SmOAKqzGtTtvt1Bg4-5rFen2Beydha1_' AS row 
    WITH row
    WHERE row.edition IS NOT NULL
    MERGE (:Edition {title: row.edition, year: toInteger(row.edition_year)})
                         """)  

    # Keyword
    # https://drive.google.com/file/d/1I23KRTezmpz03D8XDWZOz46WLuYQrdb0/view?usp=sharing
    driver.execute_query("""
    LOAD CSV WITH HEADERS FROM 'https://drive.google.com/uc?=download&id=1I23KRTezmpz03D8XDWZOz46WLuYQrdb0' AS row 
    WITH row
    WHERE row.row.Keyword IS NOT NULL
    MERGE (:Keyword {tag: row.Keyword})
                         """)  

    # EDGES

    # Co-Writes edge
    #https://drive.google.com/file/d/1NSfmBRordGMIPFNofZBNNVeLmZcDM4yV/view?usp=sharing
    driver.execute_query("""
    LOAD CSV WITH HEADERS FROM 'https://drive.google.com/uc?=download&id=1plOozr2Zp_QsYGE4JNGdgaiRbIRIQwzp' AS row 
    MATCH (author:Author {full_names: row.co_author}), (paper:Paper {title: row.paper})
    MERGE (author)-[:CO_WRITES]->(paper) 
                         """) 

    # Writes edges
    # https://drive.google.com/file/d/1plOozr2Zp_QsYGE4JNGdgaiRbIRIQwzp/view?usp=sharing
    driver.execute_query("""
    LOAD CSV WITH HEADERS FROM 'https://drive.google.com/uc?=download&id=1plOozr2Zp_QsYGE4JNGdgaiRbIRIQwzp' AS row 
    MATCH (author:Author {full_names: row.main_author}), (paper:Paper {title: row.paper})
    MERGE (author)-[:WRITES]->(paper) 
                         """)

    # "has" edge
    # https://drive.google.com/file/d/1no7tPEEIyX9oDZRApaS97f9-6pSeaRQD/view?usp=sharing
    driver.execute_query("""
    LOAD CSV WITH HEADERS FROM 'https://drive.google.com/uc?=download&id=1no7tPEEIyX9oDZRApaS97f9-6pSeaRQD' AS row 
    MATCH (journal:Journal {name: row.journal}), (vol:Volume {id: row.volume})
    MERGE (journal)-[r:HAS]->(vol)
                         """) 

    # Reviews edge
    #https://drive.google.com/file/d/1IjJKiSCLITv-FhuazwQz3akhQ80iomYj/view?usp=sharing
    driver.execute_query("""
    LOAD CSV WITH HEADERS FROM 'https://drive.google.com/uc?=download&id=1IjJKiSCLITv-FhuazwQz3akhQ80iomYj' AS row 
    MATCH (author:Author {full_names: row.reviewers}), (paper:Paper {title: row.paper})
    MERGE (author)-[:REVIEWS {date: row.date}]->(paper) 
                         """) 

    # "Cites" edge
    # https://drive.google.com/file/d/19U0lTvi4w97lPAMcdvNFLUcvT5LDcM-Z/view?usp=sharing

    driver.execute_query("""
    LOAD CSV WITH HEADERS FROM 'https://drive.google.com/uc?=download&id=19U0lTvi4w97lPAMcdvNFLUcvT5LDcM-Z' AS row 
    MATCH (paper1:Paper {title: row.paper}), (paper2:Paper {title: row.cites})
    MERGE (paper1)-[r:CITES]->(paper2)
                         """)

    # "of" edge (same file of keyword)
    # https://drive.google.com/file/d/1I23KRTezmpz03D8XDWZOz46WLuYQrdb0/view?usp=sharing
    driver.execute_query("""
    LOAD CSV WITH HEADERS FROM 'https://drive.google.com/uc?=download&id=1I23KRTezmpz03D8XDWZOz46WLuYQrdb0' AS row 
    WITH row
    WHERE row.row.Keyword IS NOT NULL
    MATCH (key:Keyword {tag: row.Keyword}), (paper:Paper {title: row.Keyword})
                         """)  

    # 