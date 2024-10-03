import pytest

import ixmp4
from ixmp4.data.abstract import Docs


class TestDataDocs:
    def test_get_and_set_modeldocs(self, platform: ixmp4.Platform):
        model = platform.backend.models.create("Model")

        docs_model = platform.backend.models.docs.set(model.id, "Description of Model")
        docs_model1 = platform.backend.models.docs.get(model.id)
        assert docs_model == docs_model1

    def test_change_empty_modeldocs(self, platform: ixmp4.Platform):
        model = platform.backend.models.create("Model")

        with pytest.raises(Docs.NotFound):
            platform.backend.models.docs.get(model.id)

        docs_model1 = platform.backend.models.docs.set(
            model.id, "Description of test Model"
        )

        assert platform.backend.models.docs.get(model.id) == docs_model1

        docs_model2 = platform.backend.models.docs.set(
            model.id, "Different description of test Model"
        )

        assert platform.backend.models.docs.get(model.id) == docs_model2

    def test_delete_modeldocs(self, platform: ixmp4.Platform):
        model = platform.backend.models.create("Model")
        docs_model = platform.backend.models.docs.set(
            model.id, "Description of test Model"
        )

        assert platform.backend.models.docs.get(model.id) == docs_model

        platform.backend.models.docs.delete(model.id)

        with pytest.raises(Docs.NotFound):
            platform.backend.models.docs.get(model.id)

    def test_get_and_set_regiondocs(self, platform: ixmp4.Platform):
        region = platform.backend.regions.create("Region", "Hierarchy")
        docs_region = platform.backend.regions.docs.set(
            region.id, "Description of test Region"
        )
        docs_region1 = platform.backend.regions.docs.get(region.id)

        assert docs_region == docs_region1

    def test_change_empty_regiondocs(self, platform: ixmp4.Platform):
        region = platform.backend.regions.create("Region", "Hierarchy")

        with pytest.raises(Docs.NotFound):
            platform.backend.regions.docs.get(region.id)

        docs_region1 = platform.backend.regions.docs.set(
            region.id, "Description of test region"
        )

        assert platform.backend.regions.docs.get(region.id) == docs_region1

        docs_region2 = platform.backend.regions.docs.set(
            region.id, "Different description of test region"
        )

        assert platform.backend.regions.docs.get(region.id) == docs_region2

    def test_delete_regiondocs(self, platform: ixmp4.Platform):
        region = platform.backend.regions.create("Region", "Hierarchy")
        docs_region = platform.backend.regions.docs.set(
            region.id, "Description of test region"
        )

        assert platform.backend.regions.docs.get(region.id) == docs_region

        platform.backend.regions.docs.delete(region.id)

        with pytest.raises(Docs.NotFound):
            platform.backend.regions.docs.get(region.id)

    def test_get_and_set_scenariodocs(self, platform: ixmp4.Platform):
        scenario = platform.backend.scenarios.create("Scenario")
        docs_scenario = platform.backend.scenarios.docs.set(
            scenario.id, "Description of Scenario"
        )
        docs_scenario1 = platform.backend.scenarios.docs.get(scenario.id)
        assert docs_scenario == docs_scenario1

    def test_change_empty_scenariodocs(self, platform: ixmp4.Platform):
        scenario = platform.backend.scenarios.create("Scenario")

        with pytest.raises(Docs.NotFound):
            platform.backend.scenarios.docs.get(scenario.id)

        docs_scenario1 = platform.backend.scenarios.docs.set(
            scenario.id, "Description of test Scenario"
        )

        assert platform.backend.scenarios.docs.get(scenario.id) == docs_scenario1

        docs_scenario2 = platform.backend.scenarios.docs.set(
            scenario.id, "Different description of test Scenario"
        )

        assert platform.backend.scenarios.docs.get(scenario.id) == docs_scenario2

    def test_delete_scenariodocs(self, platform: ixmp4.Platform):
        scenario = platform.backend.scenarios.create("Scenario")
        docs_scenario = platform.backend.scenarios.docs.set(
            scenario.id, "Description of test Scenario"
        )

        assert platform.backend.scenarios.docs.get(scenario.id) == docs_scenario

        platform.backend.scenarios.docs.delete(scenario.id)

        with pytest.raises(Docs.NotFound):
            platform.backend.scenarios.docs.get(scenario.id)

    def test_get_and_set_unitdocs(self, platform: ixmp4.Platform):
        unit = platform.backend.units.create("Unit")
        docs_unit = platform.backend.units.docs.set(unit.id, "Description of test Unit")
        docs_unit1 = platform.backend.units.docs.get(unit.id)

        assert docs_unit == docs_unit1

    def test_change_empty_unitdocs(self, platform: ixmp4.Platform):
        unit = platform.backend.units.create("Unit")

        with pytest.raises(Docs.NotFound):
            platform.backend.units.docs.get(unit.id)

        docs_unit1 = platform.backend.units.docs.set(
            unit.id, "Description of test Unit"
        )

        assert platform.backend.units.docs.get(unit.id) == docs_unit1

        docs_unit2 = platform.backend.units.docs.set(
            unit.id, "Different description of test Unit"
        )

        assert platform.backend.units.docs.get(unit.id) == docs_unit2

    def test_delete_unitdocs(self, platform: ixmp4.Platform):
        unit = platform.backend.units.create("Unit")
        docs_unit = platform.backend.units.docs.set(unit.id, "Description of test Unit")

        assert platform.backend.units.docs.get(unit.id) == docs_unit

        platform.backend.units.docs.delete(unit.id)

        with pytest.raises(Docs.NotFound):
            platform.backend.units.docs.get(unit.id)

    def test_get_and_set_variabledocs(self, platform: ixmp4.Platform):
        variable = platform.backend.iamc.variables.create("Variable")
        docs_variable = platform.backend.iamc.variables.docs.set(
            variable.id, "Description of test Variable"
        )
        docs_variables1 = platform.backend.iamc.variables.docs.get(variable.id)

        assert docs_variable == docs_variables1

    def test_change_empty_variabledocs(self, platform: ixmp4.Platform):
        variable = platform.backend.iamc.variables.create("Variable")

        with pytest.raises(Docs.NotFound):
            platform.backend.iamc.variables.docs.get(variable.id)

        docs_variable1 = platform.backend.iamc.variables.docs.set(
            variable.id, "Description of test Variable"
        )

        assert platform.backend.iamc.variables.docs.get(variable.id) == docs_variable1

        docs_variable2 = platform.backend.iamc.variables.docs.set(
            variable.id, "Different description of test Variable"
        )

        assert platform.backend.iamc.variables.docs.get(variable.id) == docs_variable2

    def test_delete_variabledocs(self, platform: ixmp4.Platform):
        variable = platform.backend.iamc.variables.create("Variable")
        docs_variable = platform.backend.iamc.variables.docs.set(
            variable.id, "Description of test Variable"
        )

        assert platform.backend.iamc.variables.docs.get(variable.id) == docs_variable

        platform.backend.iamc.variables.docs.delete(variable.id)

        with pytest.raises(Docs.NotFound):
            platform.backend.iamc.variables.docs.get(variable.id)

    def test_get_and_set_indexsetdocs(self, platform: ixmp4.Platform):
        run = platform.backend.runs.create("Model", "Scenario")
        indexset = platform.backend.optimization.indexsets.create(
            run_id=run.id, name="IndexSet"
        )
        docs_indexset = platform.backend.optimization.indexsets.docs.set(
            indexset.id, "Description of test IndexSet"
        )
        docs_indexset1 = platform.backend.optimization.indexsets.docs.get(indexset.id)

        assert docs_indexset == docs_indexset1

    def test_change_empty_indexsetdocs(self, platform: ixmp4.Platform):
        run = platform.backend.runs.create("Model", "Scenario")
        indexset = platform.backend.optimization.indexsets.create(
            run_id=run.id, name="IndexSet"
        )

        with pytest.raises(Docs.NotFound):
            platform.backend.optimization.indexsets.docs.get(indexset.id)

        docs_indexset1 = platform.backend.optimization.indexsets.docs.set(
            indexset.id, "Description of test IndexSet"
        )

        assert (
            platform.backend.optimization.indexsets.docs.get(indexset.id)
            == docs_indexset1
        )

        docs_indexset2 = platform.backend.optimization.indexsets.docs.set(
            indexset.id, "Different description of test IndexSet"
        )

        assert (
            platform.backend.optimization.indexsets.docs.get(indexset.id)
            == docs_indexset2
        )

    def test_delete_indexsetdocs(self, platform: ixmp4.Platform):
        run = platform.backend.runs.create("Model", "Scenario")
        indexset = platform.backend.optimization.indexsets.create(
            run_id=run.id, name="IndexSet"
        )
        docs_indexset = platform.backend.optimization.indexsets.docs.set(
            indexset.id, "Description of test IndexSet"
        )

        assert (
            platform.backend.optimization.indexsets.docs.get(indexset.id)
            == docs_indexset
        )

        platform.backend.optimization.indexsets.docs.delete(indexset.id)

        with pytest.raises(Docs.NotFound):
            platform.backend.optimization.indexsets.docs.get(indexset.id)

    def test_get_and_set_scalardocs(self, platform: ixmp4.Platform):
        run = platform.backend.runs.create("Model", "Scenario")
        unit = platform.backend.units.create("Unit")
        scalar = platform.backend.optimization.scalars.create(
            run_id=run.id, name="Scalar", value=1, unit_name=unit.name
        )
        docs_scalar = platform.backend.optimization.scalars.docs.set(
            scalar.id, "Description of test Scalar"
        )
        docs_scalar1 = platform.backend.optimization.scalars.docs.get(scalar.id)

        assert docs_scalar == docs_scalar1

    def test_change_empty_scalardocs(self, platform: ixmp4.Platform):
        run = platform.backend.runs.create("Model", "Scenario")
        unit = platform.backend.units.create("Unit")
        scalar = platform.backend.optimization.scalars.create(
            run_id=run.id, name="Scalar", value=2, unit_name=unit.name
        )

        with pytest.raises(Docs.NotFound):
            platform.backend.optimization.scalars.docs.get(scalar.id)

        docs_scalar1 = platform.backend.optimization.scalars.docs.set(
            scalar.id, "Description of test Scalar"
        )

        assert platform.backend.optimization.scalars.docs.get(scalar.id) == docs_scalar1

        docs_scalar2 = platform.backend.optimization.scalars.docs.set(
            scalar.id, "Different description of test Scalar"
        )

        assert platform.backend.optimization.scalars.docs.get(scalar.id) == docs_scalar2

    def test_delete_scalardocs(self, platform: ixmp4.Platform):
        run = platform.backend.runs.create("Model", "Scenario")
        unit = platform.backend.units.create("Unit")
        scalar = platform.backend.optimization.scalars.create(
            run_id=run.id, name="Scalar", value=3, unit_name=unit.name
        )
        docs_scalar = platform.backend.optimization.scalars.docs.set(
            scalar.id, "Description of test Scalar"
        )

        assert platform.backend.optimization.scalars.docs.get(scalar.id) == docs_scalar

        platform.backend.optimization.scalars.docs.delete(scalar.id)

        with pytest.raises(Docs.NotFound):
            platform.backend.optimization.scalars.docs.get(scalar.id)

    def test_get_and_set_tabledocs(self, platform: ixmp4.Platform):
        run = platform.backend.runs.create("Model", "Scenario")
        _ = platform.backend.optimization.indexsets.create(
            run_id=run.id, name="Indexset"
        )
        table = platform.backend.optimization.tables.create(
            run_id=run.id, name="Table", constrained_to_indexsets=["Indexset"]
        )
        docs_table = platform.backend.optimization.tables.docs.set(
            table.id, "Description of test Table"
        )
        docs_table1 = platform.backend.optimization.tables.docs.get(table.id)

        assert docs_table == docs_table1

    def test_change_empty_tabledocs(self, platform: ixmp4.Platform):
        run = platform.backend.runs.create("Model", "Scenario")
        _ = platform.backend.optimization.indexsets.create(
            run_id=run.id, name="Indexset"
        )
        table = platform.backend.optimization.tables.create(
            run_id=run.id, name="Table", constrained_to_indexsets=["Indexset"]
        )

        with pytest.raises(Docs.NotFound):
            platform.backend.optimization.tables.docs.get(table.id)

        docs_table1 = platform.backend.optimization.tables.docs.set(
            table.id, "Description of test Table"
        )

        assert platform.backend.optimization.tables.docs.get(table.id) == docs_table1

        docs_table2 = platform.backend.optimization.tables.docs.set(
            table.id, "Different description of test Table"
        )

        assert platform.backend.optimization.tables.docs.get(table.id) == docs_table2

    def test_delete_tabledocs(self, platform: ixmp4.Platform):
        run = platform.backend.runs.create("Model", "Scenario")
        _ = platform.backend.optimization.indexsets.create(
            run_id=run.id, name="Indexset"
        )
        table = platform.backend.optimization.tables.create(
            run_id=run.id, name="Table", constrained_to_indexsets=["Indexset"]
        )
        docs_table = platform.backend.optimization.tables.docs.set(
            table.id, "Description of test Table"
        )

        assert platform.backend.optimization.tables.docs.get(table.id) == docs_table

        platform.backend.optimization.tables.docs.delete(table.id)

        with pytest.raises(Docs.NotFound):
            platform.backend.optimization.tables.docs.get(table.id)

    def test_get_and_set_parameterdocs(self, platform: ixmp4.Platform):
        run = platform.backend.runs.create("Model", "Scenario")
        _ = platform.backend.optimization.indexsets.create(
            run_id=run.id, name="Indexset"
        )
        parameter = platform.backend.optimization.parameters.create(
            run_id=run.id, name="Parameter", constrained_to_indexsets=["Indexset"]
        )
        docs_parameter = platform.backend.optimization.parameters.docs.set(
            parameter.id, "Description of test Parameter"
        )
        docs_parameter1 = platform.backend.optimization.parameters.docs.get(
            parameter.id
        )

        assert docs_parameter == docs_parameter1

    def test_change_empty_parameterdocs(self, platform: ixmp4.Platform):
        run = platform.backend.runs.create("Model", "Scenario")
        _ = platform.backend.optimization.indexsets.create(
            run_id=run.id, name="Indexset"
        )
        parameter = platform.backend.optimization.parameters.create(
            run_id=run.id, name="Parameter", constrained_to_indexsets=["Indexset"]
        )

        with pytest.raises(Docs.NotFound):
            platform.backend.optimization.parameters.docs.get(parameter.id)

        docs_parameter1 = platform.backend.optimization.parameters.docs.set(
            parameter.id, "Description of test Parameter"
        )

        assert (
            platform.backend.optimization.parameters.docs.get(parameter.id)
            == docs_parameter1
        )

        docs_parameter2 = platform.backend.optimization.parameters.docs.set(
            parameter.id, "Different description of test Parameter"
        )

        assert (
            platform.backend.optimization.parameters.docs.get(parameter.id)
            == docs_parameter2
        )

    def test_delete_parameterdocs(self, platform: ixmp4.Platform):
        run = platform.backend.runs.create("Model", "Scenario")
        _ = platform.backend.optimization.indexsets.create(
            run_id=run.id, name="Indexset"
        )
        parameter = platform.backend.optimization.parameters.create(
            run_id=run.id, name="Parameter", constrained_to_indexsets=["Indexset"]
        )
        docs_parameter = platform.backend.optimization.parameters.docs.set(
            parameter.id, "Description of test Parameter"
        )

        assert (
            platform.backend.optimization.parameters.docs.get(parameter.id)
            == docs_parameter
        )

        platform.backend.optimization.parameters.docs.delete(parameter.id)

        with pytest.raises(Docs.NotFound):
            platform.backend.optimization.parameters.docs.get(parameter.id)

    def test_get_and_set_optimizationvariabledocs(self, platform: ixmp4.Platform):
        run = platform.backend.runs.create("Model", "Scenario")
        _ = platform.backend.optimization.indexsets.create(
            run_id=run.id, name="Indexset"
        )
        variable = platform.backend.optimization.variables.create(
            run_id=run.id, name="Variable", constrained_to_indexsets=["Indexset"]
        )
        docs_variable = platform.backend.optimization.variables.docs.set(
            variable.id, "Description of test Variable"
        )
        docs_variable1 = platform.backend.optimization.variables.docs.get(variable.id)

        assert docs_variable == docs_variable1

    def test_change_empty_optimizationvariabledocs(self, platform: ixmp4.Platform):
        run = platform.backend.runs.create("Model", "Scenario")
        _ = platform.backend.optimization.indexsets.create(
            run_id=run.id, name="Indexset"
        )
        variable = platform.backend.optimization.variables.create(
            run_id=run.id, name="Variable", constrained_to_indexsets=["Indexset"]
        )

        with pytest.raises(Docs.NotFound):
            platform.backend.optimization.variables.docs.get(variable.id)

        docs_variable1 = platform.backend.optimization.variables.docs.set(
            variable.id, "Description of test Variable"
        )

        assert (
            platform.backend.optimization.variables.docs.get(variable.id)
            == docs_variable1
        )

        docs_variable2 = platform.backend.optimization.variables.docs.set(
            variable.id, "Different description of test Variable"
        )

        assert (
            platform.backend.optimization.variables.docs.get(variable.id)
            == docs_variable2
        )

    def test_delete_optimizationvariabledocs(self, platform: ixmp4.Platform):
        run = platform.backend.runs.create("Model", "Scenario")
        _ = platform.backend.optimization.indexsets.create(
            run_id=run.id, name="Indexset"
        )
        variable = platform.backend.optimization.variables.create(
            run_id=run.id, name="Variable", constrained_to_indexsets=["Indexset"]
        )
        docs_variable = platform.backend.optimization.variables.docs.set(
            variable.id, "Description of test Variable"
        )

        assert (
            platform.backend.optimization.variables.docs.get(variable.id)
            == docs_variable
        )

        platform.backend.optimization.variables.docs.delete(variable.id)

        with pytest.raises(Docs.NotFound):
            platform.backend.optimization.variables.docs.get(variable.id)
