import json
import logging
import re
from typing import Dict, List, Optional

import deepdiff
from datahub.configuration.common import AllowDenyPattern

from datahub_actions.event.event_registry import MetadataChangeLogEvent


def metadata_change_event_diff(event: MetadataChangeLogEvent) -> Optional[Dict]:
    aspect_value = json.loads(event.aspect.value)
    try:
        previous_aspect_value = json.loads(event.previousAspectValue.value)
    except AttributeError:
        # previousAspectValue won't exist in the case of a blank schema getting
        # its first field description so just make it an empty string
        previous_aspect_value = ""
    return _get_aspect_value_diff(previous_aspect_value, aspect_value)


def _get_aspect_value_diff(
    prev_aspect_value: Dict, aspect_value: Dict
) -> Optional[Dict]:
    diff = deepdiff.DeepDiff(
        prev_aspect_value, aspect_value, ignore_order=True, view="tree"
    )
    if "iterable_item_added" in diff:
        # new desc added to field in schema that already has other field desc
        for item in diff["iterable_item_added"]:
            if "fieldPath" in item.t2:
                return item.t2
    if "values_changed" in diff:
        # field desc changed in schema
        for item in diff["values_changed"]:
            if "fieldPath" in item.up.t2:
                return item.up.t2
    if "type_changes" in diff:
        # new field desc added to schema that had no previous field desc
        for item in diff["type_changes"]:
            change = item.t2
            if "editableSchemaFieldInfo" in change:
                return change["editableSchemaFieldInfo"][0]


def is_excluded_field(
    diff_dict: Optional[Dict], excluded_fields: Optional[List[str]]
) -> bool:
    if diff_dict and excluded_fields:
        for field in excluded_fields:
            if diff_dict["fieldPath"] == field:
                return True
            else:
                return False
    else:
        return False


def is_allowed_dataset(urn_dict: Dict, pattern: Optional[AllowDenyPattern]) -> bool:
    if urn_dict and pattern:
        return pattern.allowed(
            f"{urn_dict['data_platform']}.{urn_dict['dataset']}".upper()
        )


def entity_urn_parse(entity_urn: str) -> Dict:
    # some of the entity urns are structured differently
    # we need to extract the platform and dataset for
    # allow/deny pattern matching
    # the index below will return True for urns that
    # follow the convention urn:li:dataset:(urn:li:dataPlatform:...
    entity_index = {
        "chart": False,
        "dashboard": False,
        "dataFlow": False,
        "dataJob": True,
        "dataset": True,
    }
    if entity_urn:
        try:
            parsed_urn = entity_urn.split(":")
            if not entity_index[parsed_urn[2]]:
                parsed_platform = re.sub(r"[()]", "", str(parsed_urn[3].split(",")[0]))
                parsed_entity = re.sub(r"[()]", "", str(parsed_urn[3].split(",")[1]))
                return {"data_platform": parsed_platform, "dataset": parsed_entity}

            parse_parsed_urn = parsed_urn[6].split(",")
            return {
                "data_platform": parse_parsed_urn[0],
                "dataset": parse_parsed_urn[1],
            }
        except Exception as e:
            logging.error(f"Error parsing urn for pattern match: {e}")


def parse_remove_platform(parent_dataset_lineage: Dict) -> Dict:
    parent_dataset_urn_parse = entity_urn_parse(parent_dataset_lineage["entity"]["urn"])
    parent_data_platform = parent_dataset_urn_parse["data_platform"]

    for entity in parent_dataset_lineage["searchAcrossLineage"]["searchResults"][:]:
        lineage_entity_urn_parse = entity_urn_parse(entity["entity"]["urn"])
        lineage_entity_data_platform = lineage_entity_urn_parse["data_platform"]

        if lineage_entity_data_platform != parent_data_platform:
            parent_dataset_lineage["searchAcrossLineage"]["searchResults"].pop(
                parent_dataset_lineage["searchAcrossLineage"]["searchResults"].index(
                    entity
                )
            )

    return parent_dataset_lineage


def parse_remove_entity_type(
    parent_dataset_lineage: Dict, entity_type: List[str]
) -> Dict:
    for entity in parent_dataset_lineage["searchAcrossLineage"]["searchResults"][:]:
        lineage_entity_type = entity["entity"]["type"].lower()

        if lineage_entity_type not in entity_type:
            parent_dataset_lineage["searchAcrossLineage"]["searchResults"].pop(
                parent_dataset_lineage["searchAcrossLineage"]["searchResults"].index(
                    entity
                )
            )

    return parent_dataset_lineage


def parse_deny_dataset(
    parent_dataset_lineage: Dict, pattern: Optional[AllowDenyPattern]
) -> Dict:
    if pattern:
        for entity in parent_dataset_lineage["searchAcrossLineage"]["searchResults"][:]:
            parsed_urn = entity_urn_parse(entity["entity"]["urn"])
            if not pattern.allowed(
                f"{parsed_urn['data_platform']}.{parsed_urn['dataset']}".upper()
            ):
                parent_dataset_lineage["searchAcrossLineage"]["searchResults"].pop(
                    parent_dataset_lineage["searchAcrossLineage"][
                        "searchResults"
                    ].index(entity)
                )
    return parent_dataset_lineage


def parse_desc_field(
    parent_dataset_lineage: Dict, change_event_diff: Dict
) -> List[Dict]:
    entities = []
    for entity in parent_dataset_lineage["searchAcrossLineage"]["searchResults"]:
        if (
            "schemaMetadata" in entity["entity"]
            and "fields" in entity["entity"]["schemaMetadata"]
        ):
            for field in entity["entity"]["schemaMetadata"]["fields"]:
                if field["fieldPath"] == change_event_diff["fieldPath"]:
                    entities.append(
                        {
                            "urn": entity["entity"]["urn"],
                            "field_path": field["fieldPath"],
                            "description": change_event_diff["description"],
                        }
                    )
    return entities


def create_propagation_success_log_message(
    propagation_dict: Dict, urn: str, actor: str
) -> str:
    return f"Dataset: {urn} was updated by {actor} and successfully propagated to: {propagation_dict}"
