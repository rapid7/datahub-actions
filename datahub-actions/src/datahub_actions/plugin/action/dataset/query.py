def build_schema_docu_mutation_query(urn, desc):
    query = {
        "query": f"""mutation updateDataset {{updateDataset(urn: "{urn}", input: {{editableProperties: {{description: test }}}}) {{urn}}}}"""
    }
    return query