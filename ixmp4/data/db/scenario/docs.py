from ..docs import docs_model, BaseDocsRepository
from .model import Scenario


class ScenarioDocsRepository(BaseDocsRepository):
    model_class = docs_model(Scenario)  # ScenarioDocs
    dimension_model_class = Scenario
