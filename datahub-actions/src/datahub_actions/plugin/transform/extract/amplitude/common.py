import json
import logging
import re
import unicodedata
from typing import Dict

from deepdiff import DeepDiff

logger = logging.getLogger(__name__)


def _validate_description_update(event: Dict) -> bool:
    """
    Users can change the formatting of a description,
    bold, italic, etc. and add images or links.
    We're filtering out those changes that won't play
    well with an api update to amplitude
    """
    match = False

    if re.search("[*~_`]", event["description"]):
        # bold, italic, strikethrough, code block
        match = True
    elif re.search("1\\.", event["description"]):
        # bullet point, numbered list
        match = True
    elif re.search("!\\[", event["description"]):
        # image
        match = True
    elif re.search("<br>", event["description"]):
        # table
        match = True
    return match


def _get_update_type(event: Dict) -> None:
    """
    If the description is a clickable link, the text needs
    to be extracted, so it can be written back to Amplitude
    """
    if re.search("\\[", event["description"]):
        desc_list = re.findall("\\[.*?]", event["description"])
        event["description"] = desc_list[0].strip("[]")


def _parse_amplitude_event_from_entity_urn(event: Dict) -> str:
    entity_urn = event["event"]["entityUrn"]
    urn_list = re.findall("\\((.*?)\\)", entity_urn)
    return urn_list[0].split(",")[1]


def update_description_diff_check(event: Dict) -> Dict:
    new_value = event["event"]["aspect"]["value"]
    aspect_value = json.loads(new_value)
    new_field_info = aspect_value["editableSchemaFieldInfo"]

    prev_value = event["event"]["previousAspectValue"]["value"]
    prev_aspect_value = json.loads(prev_value)
    prev_field_info = prev_aspect_value["editableSchemaFieldInfo"]

    update_values = {}

    for field in new_field_info:
        for prev_field in prev_field_info:
            if field["fieldPath"] == prev_field["fieldPath"]:
                if DeepDiff(field, prev_field):
                    update_values["event_property"] = field["fieldPath"]
                    update_values["description"] = unicodedata.normalize(
                        "NFKD", field["description"].strip()
                    )
    update_values["amp_event"] = _parse_amplitude_event_from_entity_urn(event)

    # make sure users are not adding any funky formatting
    if _validate_description_update(update_values):
        logger.error("Update contains a format not valid with Amplitude")
    else:
        # only add the changes if they are not
        _get_update_type(update_values)
        event["meta"]["amplitude"] = update_values
    return event
