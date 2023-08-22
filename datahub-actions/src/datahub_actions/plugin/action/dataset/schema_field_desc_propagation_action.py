import logging
from typing import List, Optional

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from pydantic import Field

from datahub_actions.action.action import Action
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import MetadataChangeLogEvent
from datahub_actions.pipeline.pipeline_context import PipelineContext
from datahub_actions.plugin.action.dataset.common import (
    create_propagation_success_log_message,
    entity_urn_parse,
    is_allowed_dataset,
    is_excluded_field,
    metadata_change_event_diff,
    parse_deny_dataset,
    parse_desc_field,
    parse_remove_entity_type,
    parse_remove_platform,
)
from datahub_actions.plugin.action.dataset.query import (
    build_mutation_query_body,
    build_update_desc_mutation_query,
    make_search_lineage_query,
)

logger = logging.getLogger(__name__)


class FieldDescriptionPropagationConfig(ConfigModel):
    enabled: bool = Field(
        True, description="Whether field description propagation is enabled"
    )
    parent_pattern: AllowDenyPattern = Field(
        description="A list of parent dataset urns from which propagation is allowed only"
    )
    child_pattern: Optional[AllowDenyPattern] = Field(
        description="An optional list of child dataset urns to exclude from propagation"
    )
    entity_type: List[str] = Field(
        description="The entity type to allow propagation to",
        example="dataset, chart, dashboard, dataflow, datajob",
    )
    direction: str = Field(
        description="The direction which propagation is to take place",
        example="UPSTREAM, DOWNSTREAM",
    )
    dependency_level: List[str] = Field(
        description="The level of depth that propagation is to go. Can be ['1'] or ['1', '2', '3+']"
    )
    exclude_fields: Optional[List[str]] = Field(
        description="A list of field names that should not be propagated to (is overriden by child pattern exclusion)"
    )
    platform_propagate: bool = Field(
        description="Whether to propagate field descriptions to different sources. Child pattern exclude can be used."
    )


class SchemaFieldDescriptionPropagationActionConfig(ConfigModel):
    field_description: FieldDescriptionPropagationConfig = Field(
        description="The config for field propagation"
    )


class SchemaFieldDescriptionPropagationAction(Action):
    def __init__(
        self,
        config: SchemaFieldDescriptionPropagationActionConfig,
        ctx: PipelineContext,
    ):
        self.config = config
        self.ctx = ctx

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Action":
        action_config = SchemaFieldDescriptionPropagationActionConfig.parse_obj(
            config_dict or {}
        )
        return cls(action_config, ctx)

    def act(self, event: EventEnvelope) -> None:
        try:
            assert isinstance(
                event.event, MetadataChangeLogEvent
            ), "Not a MetadataChangeLogEvent, skipping"
            if self.config.field_description.enabled:
                if (
                    event.event.aspectName == "editableSchemaMetadata"
                    and event.event.entityType == "dataset"
                    and event.event.created.actor != "urn:li:corpuser:datahub"
                ):
                    # is the dataset a parent from which to propagate
                    if is_allowed_dataset(
                        entity_urn_parse(event.event.entityUrn),
                        self.config.field_description.parent_pattern,
                    ):
                        logger.info(
                            f"Starting propagation for dataset: {event.event.entityUrn}"
                        )

                        # get the field update
                        change_event_diff = metadata_change_event_diff(event.event)

                        # is there a diff and is it on the excluded fields list
                        if change_event_diff and not is_excluded_field(
                            change_event_diff,
                            self.config.field_description.exclude_fields,
                        ):
                            if (
                                "fieldPath" in change_event_diff
                                and "description" in change_event_diff
                            ):
                                # we have a field update and should get the lineage
                                # of the parent dataset
                                parent_dataset_lineage = make_search_lineage_query(
                                    event.event.entityUrn,
                                    self.config.field_description.direction,
                                    self.config.field_description.dependency_level,
                                    self.ctx.graph,
                                )

                                if "searchAcrossLineage" in parent_dataset_lineage:
                                    if (
                                        len(
                                            parent_dataset_lineage[
                                                "searchAcrossLineage"
                                            ]["searchResults"]
                                        )
                                        > 0
                                    ):
                                        # if propagate to platforms different to the root dataset
                                        # is false, let's remove any instances of those from the
                                        # lineage response
                                        if (
                                            not self.config.field_description.platform_propagate
                                        ):
                                            parse_remove_platform(
                                                parent_dataset_lineage
                                            )

                                        # remove any datasets in the deny list
                                        if self.config.field_description.child_pattern:
                                            parse_deny_dataset(
                                                parent_dataset_lineage,
                                                self.config.field_description.child_pattern,
                                            )

                                        # remove any entities not on the entity list
                                        parse_remove_entity_type(
                                            parent_dataset_lineage,
                                            self.config.field_description.entity_type,
                                        )

                                        # remove fields that aren't being propagated to
                                        schema_field_update_list = parse_desc_field(
                                            parent_dataset_lineage, change_event_diff
                                        )

                                        # build the mutation query
                                        mutation_query = (
                                            build_update_desc_mutation_query(
                                                build_mutation_query_body(
                                                    schema_field_update_list
                                                )
                                            )
                                        )

                                        # make the request, response will be empty dict in the case of non 200 or exception
                                        response = self.ctx.graph.get_by_graphql_query(
                                            mutation_query
                                        )

                                        if response:
                                            logger.info(
                                                f"{create_propagation_success_log_message(response, event.event.entityUrn, event.event.created.actor)}"
                                            )

                                    else:
                                        logger.info(
                                            f"Parent dataset has no {self.config.field_description.direction.lower()} lineage, updating parent only"
                                        )
                                else:
                                    logger.warning(
                                        "Get parent dataset lineage request has returned an empty response for searchAcrossLineage"
                                    )
                            else:
                                logger.info("Tag update, skipping...")
                        else:
                            if change_event_diff:
                                logger.info(
                                    f"Field: {change_event_diff['fieldPath']} is excluded from propagation, or there is no change to propagate"
                                )
                            else:
                                logger.info("Tag update, skipping...")
                    else:
                        print(
                            f"Field update is not on a parent urn: {event.event.entityUrn}"
                        )
        except AssertionError as e:
            logger.info(e)

    def close(self) -> None:
        return super().close()
