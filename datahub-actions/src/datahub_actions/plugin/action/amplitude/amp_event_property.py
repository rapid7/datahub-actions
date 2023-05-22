import json
import logging

import requests
from pydantic import BaseModel, SecretStr
from requests.models import HTTPBasicAuth, HTTPError

from datahub_actions.action.action import Action
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.pipeline.pipeline_context import PipelineContext
from datahub_actions.plugin.action.amplitude.api_request import ApiRequest

logger = logging.getLogger(__name__)


class AmplitudeEventPropertyActionConfig(BaseModel):
    api_key: SecretStr
    secret_key: SecretStr


class AmplitudeEventPropertyAction(Action):
    def __init__(
        self, config: AmplitudeEventPropertyActionConfig, ctx: PipelineContext
    ):
        self.ctx = ctx
        self.config = config
        self.api_request = ApiRequest()

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Action":
        action_config = AmplitudeEventPropertyActionConfig.parse_obj(config_dict or {})
        return cls(action_config, ctx)

    def act(self, event: EventEnvelope) -> None:
        event_json = json.loads(event.as_json())
        event_type = event_json.get("meta").get("amplitude").get("amp_event")
        event_property = event_json.get("meta").get("amplitude").get("event_property")
        description = event_json.get("meta").get("amplitude").get("description")

        data = {"event_type": event_type, "description": description}

        try:
            request = requests.put(
                url=f"https://amplitude.com/api/2/taxonomy/event-property/{event_property}",
                data=data,
                auth=HTTPBasicAuth(
                    self.config.api_key.get_secret_value().strip(),
                    self.config.secret_key.get_secret_value().strip(),
                ),
            )
            request.raise_for_status()
        except HTTPError as error:
            logger.debug(f"Error: {error}")

    def close(self) -> None:
        pass
