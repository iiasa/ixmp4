import pytest

import ixmp4
from ixmp4.data.abstract import Docs


class TestDataDocs:
    def test_get_and_set_modeldocs(self, platform: ixmp4.Platform) -> None:
        model = platform.backend.models.create("Model")

        docs_model = platform.backend.models.docs.set(model.id, "Description of Model")
        docs_model1 = platform.backend.models.docs.get(model.id)
        assert docs_model == docs_model1

    def test_change_empty_modeldocs(self, platform: ixmp4.Platform) -> None:
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

    def test_delete_modeldocs(self, platform: ixmp4.Platform) -> None:
        model = platform.backend.models.create("Model")
        docs_model = platform.backend.models.docs.set(
            model.id, "Description of test Model"
        )

        assert platform.backend.models.docs.get(model.id) == docs_model

        platform.backend.models.docs.delete(model.id)

        with pytest.raises(Docs.NotFound):
            platform.backend.models.docs.get(model.id)

    def test_list_modeldocs(self, platform: ixmp4.Platform) -> None:
        model_1 = platform.backend.models.create("Model 1")
        model_2 = platform.backend.models.create("Model 2")
        model_3 = platform.backend.models.create("Model 3")
        docs_model_1 = platform.backend.models.docs.set(
            model_1.id, "Description of Model 1"
        )
        docs_model_2 = platform.backend.models.docs.set(
            model_2.id, "Description of Model 2"
        )
        docs_model_3 = platform.backend.models.docs.set(
            model_3.id, "Description of Model 3"
        )

        assert platform.backend.models.docs.list() == [
            docs_model_1,
            docs_model_2,
            docs_model_3,
        ]

        assert platform.backend.models.docs.list(dimension_id=model_2.id) == [
            docs_model_2
        ]

        assert platform.backend.models.docs.list(
            dimension_id__in=[model_1.id, model_3.id]
        ) == [docs_model_1, docs_model_3]

    def test_get_and_set_regiondocs(self, platform: ixmp4.Platform) -> None:
        region = platform.backend.regions.create("Region", "Hierarchy")
        docs_region = platform.backend.regions.docs.set(
            region.id, "Description of test Region"
        )
        docs_region1 = platform.backend.regions.docs.get(region.id)

        assert docs_region == docs_region1

    def test_change_empty_regiondocs(self, platform: ixmp4.Platform) -> None:
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

    def test_delete_regiondocs(self, platform: ixmp4.Platform) -> None:
        region = platform.backend.regions.create("Region", "Hierarchy")
        docs_region = platform.backend.regions.docs.set(
            region.id, "Description of test region"
        )

        assert platform.backend.regions.docs.get(region.id) == docs_region

        platform.backend.regions.docs.delete(region.id)

        with pytest.raises(Docs.NotFound):
            platform.backend.regions.docs.get(region.id)

    def test_list_regiondocs(self, platform: ixmp4.Platform) -> None:
        region_1 = platform.backend.regions.create("Region 1", "Hierarchy")
        region_2 = platform.backend.regions.create("Region 2", "Hierarchy")
        region_3 = platform.backend.regions.create("Region 3", "Hierarchy")
        docs_region_1 = platform.backend.regions.docs.set(
            region_1.id, "Description of Region 1"
        )
        docs_region_2 = platform.backend.regions.docs.set(
            region_2.id, "Description of Region 2"
        )
        docs_region_3 = platform.backend.regions.docs.set(
            region_3.id, "Description of Region 3"
        )

        assert platform.backend.regions.docs.list() == [
            docs_region_1,
            docs_region_2,
            docs_region_3,
        ]

        assert platform.backend.regions.docs.list(dimension_id=region_2.id) == [
            docs_region_2
        ]

        assert platform.backend.regions.docs.list(
            dimension_id__in=[region_1.id, region_3.id]
        ) == [docs_region_1, docs_region_3]

    def test_get_and_set_scenariodocs(self, platform: ixmp4.Platform) -> None:
        scenario = platform.backend.scenarios.create("Scenario")
        docs_scenario = platform.backend.scenarios.docs.set(
            scenario.id, "Description of Scenario"
        )
        docs_scenario1 = platform.backend.scenarios.docs.get(scenario.id)
        assert docs_scenario == docs_scenario1

    def test_change_empty_scenariodocs(self, platform: ixmp4.Platform) -> None:
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

    def test_delete_scenariodocs(self, platform: ixmp4.Platform) -> None:
        scenario = platform.backend.scenarios.create("Scenario")
        docs_scenario = platform.backend.scenarios.docs.set(
            scenario.id, "Description of test Scenario"
        )

        assert platform.backend.scenarios.docs.get(scenario.id) == docs_scenario

        platform.backend.scenarios.docs.delete(scenario.id)

        with pytest.raises(Docs.NotFound):
            platform.backend.scenarios.docs.get(scenario.id)

    def test_list_scenariodocs(self, platform: ixmp4.Platform) -> None:
        scenario_1 = platform.backend.scenarios.create("Scenario 1")
        scenario_2 = platform.backend.scenarios.create("Scenario 2")
        scenario_3 = platform.backend.scenarios.create("Scenario 3")
        docs_scenario_1 = platform.backend.scenarios.docs.set(
            scenario_1.id, "Description of Scenario 1"
        )
        docs_scenario_2 = platform.backend.scenarios.docs.set(
            scenario_2.id, "Description of Scenario 2"
        )
        docs_scenario_3 = platform.backend.scenarios.docs.set(
            scenario_3.id, "Description of Scenario 3"
        )

        assert platform.backend.scenarios.docs.list() == [
            docs_scenario_1,
            docs_scenario_2,
            docs_scenario_3,
        ]

        assert platform.backend.scenarios.docs.list(dimension_id=scenario_2.id) == [
            docs_scenario_2
        ]

        assert platform.backend.scenarios.docs.list(
            dimension_id__in=[scenario_1.id, scenario_3.id]
        ) == [docs_scenario_1, docs_scenario_3]

    def test_get_and_set_unitdocs(self, platform: ixmp4.Platform) -> None:
        unit = platform.backend.units.create("Unit")
        docs_unit = platform.backend.units.docs.set(unit.id, "Description of test Unit")
        docs_unit1 = platform.backend.units.docs.get(unit.id)

        assert docs_unit == docs_unit1

    def test_change_empty_unitdocs(self, platform: ixmp4.Platform) -> None:
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

    def test_delete_unitdocs(self, platform: ixmp4.Platform) -> None:
        unit = platform.backend.units.create("Unit")
        docs_unit = platform.backend.units.docs.set(unit.id, "Description of test Unit")

        assert platform.backend.units.docs.get(unit.id) == docs_unit

        platform.backend.units.docs.delete(unit.id)

        with pytest.raises(Docs.NotFound):
            platform.backend.units.docs.get(unit.id)

    def test_list_unitdocs(self, platform: ixmp4.Platform) -> None:
        unit_1 = platform.backend.units.create("Unit 1")
        unit_2 = platform.backend.units.create("Unit 2")
        unit_3 = platform.backend.units.create("Unit 3")
        docs_unit_1 = platform.backend.units.docs.set(
            unit_1.id, "Description of Unit 1"
        )
        docs_unit_2 = platform.backend.units.docs.set(
            unit_2.id, "Description of Unit 2"
        )
        docs_unit_3 = platform.backend.units.docs.set(
            unit_3.id, "Description of Unit 3"
        )

        assert platform.backend.units.docs.list() == [
            docs_unit_1,
            docs_unit_2,
            docs_unit_3,
        ]

        assert platform.backend.units.docs.list(dimension_id=unit_2.id) == [docs_unit_2]

        assert platform.backend.units.docs.list(
            dimension_id__in=[unit_1.id, unit_3.id]
        ) == [docs_unit_1, docs_unit_3]

    def test_get_and_set_variabledocs(self, platform: ixmp4.Platform) -> None:
        variable = platform.backend.iamc.variables.create("Variable")
        docs_variable = platform.backend.iamc.variables.docs.set(
            variable.id, "Description of test Variable"
        )
        docs_variables1 = platform.backend.iamc.variables.docs.get(variable.id)

        assert docs_variable == docs_variables1

    def test_change_empty_variabledocs(self, platform: ixmp4.Platform) -> None:
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

    def test_delete_variabledocs(self, platform: ixmp4.Platform) -> None:
        variable = platform.backend.iamc.variables.create("Variable")
        docs_variable = platform.backend.iamc.variables.docs.set(
            variable.id, "Description of test Variable"
        )

        assert platform.backend.iamc.variables.docs.get(variable.id) == docs_variable

        platform.backend.iamc.variables.docs.delete(variable.id)

        with pytest.raises(Docs.NotFound):
            platform.backend.iamc.variables.docs.get(variable.id)

    def test_list_variabledocs(self, platform: ixmp4.Platform) -> None:
        variable_1 = platform.backend.iamc.variables.create("Variable 1")
        variable_2 = platform.backend.iamc.variables.create("Variable 2")
        variable_3 = platform.backend.iamc.variables.create("Variable 3")
        docs_variable_1 = platform.backend.iamc.variables.docs.set(
            variable_1.id, "Description of Variable 1"
        )
        docs_variable_2 = platform.backend.iamc.variables.docs.set(
            variable_2.id, "Description of Variable 2"
        )
        docs_variable_3 = platform.backend.iamc.variables.docs.set(
            variable_3.id, "Description of Variable 3"
        )

        assert platform.backend.iamc.variables.docs.list() == [
            docs_variable_1,
            docs_variable_2,
            docs_variable_3,
        ]

        assert platform.backend.iamc.variables.docs.list(
            dimension_id=variable_2.id
        ) == [docs_variable_2]

        assert platform.backend.iamc.variables.docs.list(
            dimension_id__in=[variable_1.id, variable_3.id]
        ) == [docs_variable_1, docs_variable_3]

    def test_get_and_set_indexsetdocs(self, platform: ixmp4.Platform) -> None:
        run = platform.backend.runs.create("Model", "Scenario")
        indexset = platform.backend.optimization.indexsets.create(
            run_id=run.id, name="IndexSet"
        )
        docs_indexset = platform.backend.optimization.indexsets.docs.set(
            indexset.id, "Description of test IndexSet"
        )
        docs_indexset1 = platform.backend.optimization.indexsets.docs.get(indexset.id)

        assert docs_indexset == docs_indexset1

    def test_change_empty_indexsetdocs(self, platform: ixmp4.Platform) -> None:
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

    def test_delete_indexsetdocs(self, platform: ixmp4.Platform) -> None:
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

    def test_list_indexsetdocs(self, platform: ixmp4.Platform) -> None:
        run = platform.backend.runs.create("Model", "Scenario")
        indexset_1 = platform.backend.optimization.indexsets.create(
            run_id=run.id, name="IndexSet 1"
        )
        indexset_2 = platform.backend.optimization.indexsets.create(
            run_id=run.id, name="IndexSet 2"
        )
        indexset_3 = platform.backend.optimization.indexsets.create(
            run_id=run.id, name="IndexSet 3"
        )
        docs_indexset_1 = platform.backend.optimization.indexsets.docs.set(
            indexset_1.id, "Description of IndexSet 1"
        )
        docs_indexset_2 = platform.backend.optimization.indexsets.docs.set(
            indexset_2.id, "Description of IndexSet 2"
        )
        docs_indexset_3 = platform.backend.optimization.indexsets.docs.set(
            indexset_3.id, "Description of IndexSet 3"
        )

        assert platform.backend.optimization.indexsets.docs.list() == [
            docs_indexset_1,
            docs_indexset_2,
            docs_indexset_3,
        ]

        assert platform.backend.optimization.indexsets.docs.list(
            dimension_id=indexset_2.id
        ) == [docs_indexset_2]

        assert platform.backend.optimization.indexsets.docs.list(
            dimension_id__in=[indexset_1.id, indexset_3.id]
        ) == [docs_indexset_1, docs_indexset_3]

    def test_get_and_set_scalardocs(self, platform: ixmp4.Platform) -> None:
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

    def test_change_empty_scalardocs(self, platform: ixmp4.Platform) -> None:
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

    def test_delete_scalardocs(self, platform: ixmp4.Platform) -> None:
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

    def test_list_scalardocs(self, platform: ixmp4.Platform) -> None:
        run = platform.backend.runs.create("Model", "Scenario")
        unit = platform.backend.units.create("Unit")
        scalar_1 = platform.backend.optimization.scalars.create(
            run_id=run.id, name="Scalar 1", value=1, unit_name=unit.name
        )
        scalar_2 = platform.backend.optimization.scalars.create(
            run_id=run.id, name="Scalar 2", value=2, unit_name=unit.name
        )
        scalar_3 = platform.backend.optimization.scalars.create(
            run_id=run.id, name="Scalar 3", value=3, unit_name=unit.name
        )
        docs_scalar_1 = platform.backend.optimization.scalars.docs.set(
            scalar_1.id, "Description of Scalar 1"
        )
        docs_scalar_2 = platform.backend.optimization.scalars.docs.set(
            scalar_2.id, "Description of Scalar 2"
        )
        docs_scalar_3 = platform.backend.optimization.scalars.docs.set(
            scalar_3.id, "Description of Scalar 3"
        )

        assert platform.backend.optimization.scalars.docs.list() == [
            docs_scalar_1,
            docs_scalar_2,
            docs_scalar_3,
        ]

        assert platform.backend.optimization.scalars.docs.list(
            dimension_id=scalar_2.id
        ) == [docs_scalar_2]

        assert platform.backend.optimization.scalars.docs.list(
            dimension_id__in=[scalar_1.id, scalar_3.id]
        ) == [docs_scalar_1, docs_scalar_3]

    def test_get_and_set_tabledocs(self, platform: ixmp4.Platform) -> None:
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

    def test_change_empty_tabledocs(self, platform: ixmp4.Platform) -> None:
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

    def test_delete_tabledocs(self, platform: ixmp4.Platform) -> None:
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

    def test_list_tabledocs(self, platform: ixmp4.Platform) -> None:
        run = platform.backend.runs.create("Model", "Scenario")
        indexset = platform.backend.optimization.indexsets.create(
            run_id=run.id, name="Indexset"
        )
        table_1 = platform.backend.optimization.tables.create(
            run_id=run.id, name="Table 1", constrained_to_indexsets=[indexset.name]
        )
        table_2 = platform.backend.optimization.tables.create(
            run_id=run.id, name="Table 2", constrained_to_indexsets=[indexset.name]
        )
        table_3 = platform.backend.optimization.tables.create(
            run_id=run.id, name="Table 3", constrained_to_indexsets=[indexset.name]
        )
        docs_table_1 = platform.backend.optimization.tables.docs.set(
            table_1.id, "Description of Table 1"
        )
        docs_table_2 = platform.backend.optimization.tables.docs.set(
            table_2.id, "Description of Table 2"
        )
        docs_table_3 = platform.backend.optimization.tables.docs.set(
            table_3.id, "Description of Table 3"
        )

        assert platform.backend.optimization.tables.docs.list() == [
            docs_table_1,
            docs_table_2,
            docs_table_3,
        ]

        assert platform.backend.optimization.tables.docs.list(
            dimension_id=table_2.id
        ) == [docs_table_2]

        assert platform.backend.optimization.tables.docs.list(
            dimension_id__in=[table_1.id, table_3.id]
        ) == [docs_table_1, docs_table_3]

    def test_get_and_set_parameterdocs(self, platform: ixmp4.Platform) -> None:
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

    def test_change_empty_parameterdocs(self, platform: ixmp4.Platform) -> None:
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

    def test_delete_parameterdocs(self, platform: ixmp4.Platform) -> None:
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

    def test_list_parameterdocs(self, platform: ixmp4.Platform) -> None:
        run = platform.backend.runs.create("Model", "Scenario")
        indexset = platform.backend.optimization.indexsets.create(
            run_id=run.id, name="Indexset"
        )
        parameter_1 = platform.backend.optimization.parameters.create(
            run_id=run.id, name="Parameter 1", constrained_to_indexsets=[indexset.name]
        )
        parameter_2 = platform.backend.optimization.parameters.create(
            run_id=run.id, name="Parameter 2", constrained_to_indexsets=[indexset.name]
        )
        parameter_3 = platform.backend.optimization.parameters.create(
            run_id=run.id, name="Parameter 3", constrained_to_indexsets=[indexset.name]
        )
        docs_parameter_1 = platform.backend.optimization.parameters.docs.set(
            parameter_1.id, "Description of Parameter 1"
        )
        docs_parameter_2 = platform.backend.optimization.parameters.docs.set(
            parameter_2.id, "Description of Parameter 2"
        )
        docs_parameter_3 = platform.backend.optimization.parameters.docs.set(
            parameter_3.id, "Description of Parameter 3"
        )

        assert platform.backend.optimization.parameters.docs.list() == [
            docs_parameter_1,
            docs_parameter_2,
            docs_parameter_3,
        ]

        assert platform.backend.optimization.parameters.docs.list(
            dimension_id=parameter_2.id
        ) == [docs_parameter_2]

        assert platform.backend.optimization.parameters.docs.list(
            dimension_id__in=[parameter_1.id, parameter_3.id]
        ) == [docs_parameter_1, docs_parameter_3]

    def test_get_and_set_optimizationvariabledocs(
        self, platform: ixmp4.Platform
    ) -> None:
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

    def test_change_empty_optimizationvariabledocs(
        self, platform: ixmp4.Platform
    ) -> None:
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

    def test_delete_optimizationvariabledocs(self, platform: ixmp4.Platform) -> None:
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

    def test_list_optimizationvariabledocs(self, platform: ixmp4.Platform) -> None:
        run = platform.backend.runs.create("Model", "Scenario")
        variable_1 = platform.backend.optimization.variables.create(
            run_id=run.id, name="Variable 1"
        )
        variable_2 = platform.backend.optimization.variables.create(
            run_id=run.id, name="Variable 2"
        )
        variable_3 = platform.backend.optimization.variables.create(
            run_id=run.id, name="Variable 3"
        )
        docs_variable_1 = platform.backend.optimization.variables.docs.set(
            variable_1.id, "Description of Variable 1"
        )
        docs_variable_2 = platform.backend.optimization.variables.docs.set(
            variable_2.id, "Description of Variable 2"
        )
        docs_variable_3 = platform.backend.optimization.variables.docs.set(
            variable_3.id, "Description of Variable 3"
        )

        assert platform.backend.optimization.variables.docs.list() == [
            docs_variable_1,
            docs_variable_2,
            docs_variable_3,
        ]

        assert platform.backend.optimization.variables.docs.list(
            dimension_id=variable_2.id
        ) == [docs_variable_2]

        assert platform.backend.optimization.variables.docs.list(
            dimension_id__in=[variable_1.id, variable_3.id]
        ) == [docs_variable_1, docs_variable_3]

    def test_get_and_set_equationdocs(self, platform: ixmp4.Platform) -> None:
        run = platform.backend.runs.create("Model", "Scenario")
        _ = platform.backend.optimization.indexsets.create(
            run_id=run.id, name="Indexset"
        )
        equation = platform.backend.optimization.equations.create(
            run_id=run.id, name="Equation", constrained_to_indexsets=["Indexset"]
        )
        docs_equation = platform.backend.optimization.equations.docs.set(
            equation.id, "Description of test Equation"
        )
        docs_equation1 = platform.backend.optimization.equations.docs.get(equation.id)

        assert docs_equation == docs_equation1

    def test_change_empty_equationdocs(self, platform: ixmp4.Platform) -> None:
        run = platform.backend.runs.create("Model", "Scenario")
        _ = platform.backend.optimization.indexsets.create(
            run_id=run.id, name="Indexset"
        )
        equation = platform.backend.optimization.equations.create(
            run_id=run.id, name="Equation", constrained_to_indexsets=["Indexset"]
        )

        with pytest.raises(Docs.NotFound):
            platform.backend.optimization.equations.docs.get(equation.id)

        docs_equation1 = platform.backend.optimization.equations.docs.set(
            equation.id, "Description of test Equation"
        )

        assert (
            platform.backend.optimization.equations.docs.get(equation.id)
            == docs_equation1
        )

        docs_equation2 = platform.backend.optimization.equations.docs.set(
            equation.id, "Different description of test Equation"
        )

        assert (
            platform.backend.optimization.equations.docs.get(equation.id)
            == docs_equation2
        )

    def test_delete_optimizationequationdocs(self, platform: ixmp4.Platform) -> None:
        run = platform.backend.runs.create("Model", "Scenario")
        _ = platform.backend.optimization.indexsets.create(
            run_id=run.id, name="Indexset"
        )
        equation = platform.backend.optimization.equations.create(
            run_id=run.id, name="Equation", constrained_to_indexsets=["Indexset"]
        )
        docs_equation = platform.backend.optimization.equations.docs.set(
            equation.id, "Description of test Equation"
        )

        assert (
            platform.backend.optimization.equations.docs.get(equation.id)
            == docs_equation
        )

        platform.backend.optimization.equations.docs.delete(equation.id)

        with pytest.raises(Docs.NotFound):
            platform.backend.optimization.equations.docs.get(equation.id)

    def test_list_optimizationequationdocs(self, platform: ixmp4.Platform) -> None:
        run = platform.backend.runs.create("Model", "Scenario")
        indexset = platform.backend.optimization.indexsets.create(
            run_id=run.id, name="Indexset"
        )
        equation_1 = platform.backend.optimization.equations.create(
            run_id=run.id, name="Equation 1", constrained_to_indexsets=[indexset.name]
        )
        equation_2 = platform.backend.optimization.equations.create(
            run_id=run.id, name="Equation 2", constrained_to_indexsets=[indexset.name]
        )
        equation_3 = platform.backend.optimization.equations.create(
            run_id=run.id, name="Equation 3", constrained_to_indexsets=[indexset.name]
        )
        docs_equation_1 = platform.backend.optimization.equations.docs.set(
            equation_1.id, "Description of Equation 1"
        )
        docs_equation_2 = platform.backend.optimization.equations.docs.set(
            equation_2.id, "Description of Equation 2"
        )
        docs_equation_3 = platform.backend.optimization.equations.docs.set(
            equation_3.id, "Description of Equation 3"
        )

        assert platform.backend.optimization.equations.docs.list() == [
            docs_equation_1,
            docs_equation_2,
            docs_equation_3,
        ]

        assert platform.backend.optimization.equations.docs.list(
            dimension_id=equation_2.id
        ) == [docs_equation_2]

        assert platform.backend.optimization.equations.docs.list(
            dimension_id__in=[equation_1.id, equation_3.id]
        ) == [docs_equation_1, docs_equation_3]
