from typing import Collection, Dict, List

from datahub_actions.api.action_graph import AcrylDataHubGraph
from datahub_actions.plugin.action.dataset.common import entity_urn_parse


def make_search_lineage_query(
    urn: str, direction: str, dependency_level: List[str], graph: AcrylDataHubGraph
) -> Dict:
    return graph.get_by_graphql_query(
        build_search_lineage_query_body(urn, direction, dependency_level)
    )


def build_search_lineage_query_body(
    urn: str, direction: str, dependency_level: List[str]
) -> Dict[str, Collection[str]]:
    query = {
        "query": """
            query searchLineage($urn: String!, $direction: LineageDirection!, $dependency_level: [String!]) {
              entity(urn: $urn) {
                urn
                ... on Dataset {
                  schemaMetadata(version: 0) {
                    ...schemaMetadataFields
                  }
                }
              }
            searchAcrossLineage(
              input: {query: "*", urn: $urn, start: 0, count: 1000, direction: $direction, orFilters: [{and: [{condition: EQUAL, negated: false, field: "degree", values: $dependency_level}]}]}
            ) {
            searchResults {
              degree
              entity {
                urn
                type
                ... on Dataset {
                  schemaMetadata {
                    fields {
                      fieldPath
                        type
                        nativeDataType
                    }
                  }
                }
              }
            }
          }
        }

        fragment schemaMetadataFields on SchemaMetadata {
          fields {
            ...schemaFieldFields
          }
        }

        fragment schemaFieldFields on SchemaField {
          fieldPath
          type
          nativeDataType
        }
        """,
        "variables": {
            "urn": urn,
            "direction": direction,
            "dependency_level": dependency_level,
        },
    }
    return query


def _map_variables_to_mutation_query_alias_block(
    mutation_alias: str, urn: str, field_path: str, description: str
) -> str:
    return (
        f"{mutation_alias}: updateDescription( input: "
        f'{{description: "{description}", resourceUrn: "{urn}", '
        f""
        f'subResource: "{field_path}", subResourceType:DATASET_FIELD}})'
    )


def build_mutation_query_body(field_mutation_list: List[Dict]) -> str:
    body = ""
    for field in field_mutation_list:
        # we need to escape newlines in the descriptions for the query to work
        if "\n" in field["description"]:
            description = field["description"]
            field["description"] = description.replace("\n", "\\n")
        parsed_urn_dict = entity_urn_parse(field["urn"])
        query = _map_variables_to_mutation_query_alias_block(
            parsed_urn_dict["dataset"].replace(".", "_"),
            field["urn"],
            field["field_path"],
            field["description"],
        )
        body = body + query
    return body


def build_update_desc_mutation_query(mutation_query: str) -> Dict[str, str]:
    return {"query": f"""mutation {{ {mutation_query} }}"""}
