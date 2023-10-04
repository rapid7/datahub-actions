import logging
import json
from typing import Optional, Dict, List

from datahub.configuration.common import ConfigModel
from pydantic import BaseModel, Field

from datahub_actions.action.action import Action
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.pipeline.pipeline_context import PipelineContext
from datahub_actions.plugin.action.dataset.query import build_schema_docu_mutation_query



class SchemaDocumentationPropagationActionConfig(ConfigModel):
    parent_dataset: Optional[str] = Field(
        None,
        description=""
    )
    child_dataset: Optional[str] = Field(
        None,
        description=""
    )


class SchemaDocumentationPropagationAction(Action):
    def __init__(self, config: SchemaDocumentationPropagationActionConfig, ctx: PipelineContext):
        self.config = config
        self.ctx = ctx

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Action":
        action_config = SchemaDocumentationPropagationActionConfig.parse_obj(config_dict or {})
        return cls(action_config, ctx)

    def act(self, env_event: EventEnvelope) -> None:
        pass
        event = env_event.as_json()
        event_json = json.loads(event)
        if "schema_doc" in event_json["meta"]:
            query = build_schema_docu_mutation_query(
                urn=event_json["meta"]["schema_doc"]["urn"],
                desc=event_json["meta"]["schema_doc"]["desc"]
            )
            self.ctx.graph.get_by_graphql_query(query)

    def close(self) -> None:
        return super().close()
