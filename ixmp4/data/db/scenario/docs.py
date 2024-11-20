from typing import Any

from ..docs import BaseDocsRepository, docs_model
from .model import Scenario

ScenarioDocs = docs_model(Scenario)


class ScenarioDocsRepository(BaseDocsRepository[Any]):
    model_class = ScenarioDocs
    dimension_model_class = Scenario
