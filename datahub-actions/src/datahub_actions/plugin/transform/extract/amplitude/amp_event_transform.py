import json
import logging
from typing import Any, Dict, List, Optional, Union

from datahub.configuration import ConfigModel

from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.pipeline.pipeline_context import PipelineContext
from datahub_actions.plugin.transform.extract.amplitude.common import (
    update_description_diff_check,
)
from datahub_actions.transform.transformer import Transformer

logger = logging.getLogger(__name__)


class ExtractTransformerConfig(ConfigModel):
    event_type: Union[str, List[str]]
    event: Optional[Dict[str, Any]]


class ExtractTransformer(Transformer):
    def __init__(self, ctx: PipelineContext):
        self.ctx = ctx

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Transformer":
        return cls(ctx)

    def transform(self, env_event: EventEnvelope) -> Optional[EventEnvelope]:
        logger.info(f"Preparing to extract event {env_event}")
        return env_event.from_json(
            json.dumps(update_description_diff_check(json.loads(env_event.as_json())))
        )
