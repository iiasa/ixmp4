from ..docs import BaseDocsRepository, docs_model
from .model import Scenario


class ScenarioDocsRepository(BaseDocsRepository):
    model_class = docs_model(Scenario)  # ScenarioDocs
    dimension_model_class = Scenario
