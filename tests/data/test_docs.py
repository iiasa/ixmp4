import pytest

from ixmp4.data.abstract import Docs

from ..utils import all_platforms


@all_platforms
class TestDataDocs:
    def test_get_and_set_modeldocs(self, test_mp, request):
        test_mp = request.getfixturevalue(test_mp)
        model = test_mp.backend.models.create("Model")

        docs_model = test_mp.backend.models.docs.set(model.id, "Description of Model")
        docs_model1 = test_mp.backend.models.docs.get(model.id)
        assert docs_model == docs_model1

    def test_change_empty_modeldocs(self, test_mp, request):
        test_mp = request.getfixturevalue(test_mp)
        model = test_mp.backend.models.create("Model")

        with pytest.raises(Docs.NotFound):
            test_mp.backend.models.docs.get(model.id)

        docs_model1 = test_mp.backend.models.docs.set(
            model.id, "Description of test Model"
        )

        assert test_mp.backend.models.docs.get(model.id) == docs_model1

        docs_model2 = test_mp.backend.models.docs.set(
            model.id, "Different description of test Model"
        )

        assert test_mp.backend.models.docs.get(model.id) == docs_model2

    def test_delete_modeldocs(self, test_mp, request):
        test_mp = request.getfixturevalue(test_mp)
        model = test_mp.backend.models.create("Model")
        docs_model = test_mp.backend.models.docs.set(
            model.id, "Description of test Model"
        )

        assert test_mp.backend.models.docs.get(model.id) == docs_model

        test_mp.backend.models.docs.delete(model.id)

        with pytest.raises(Docs.NotFound):
            test_mp.backend.models.docs.get(model.id)

    def test_get_and_set_regiondocs(self, test_mp, request):
        test_mp = request.getfixturevalue(test_mp)
        region = test_mp.backend.regions.create("Region", "Hierarchy")
        docs_region = test_mp.backend.regions.docs.set(
            region.id, "Description of test Region"
        )
        docs_region1 = test_mp.backend.regions.docs.get(region.id)

        assert docs_region == docs_region1

    def test_change_empty_regiondocs(self, test_mp, request):
        test_mp = request.getfixturevalue(test_mp)
        region = test_mp.backend.regions.create("Region", "Hierarchy")

        with pytest.raises(Docs.NotFound):
            test_mp.backend.regions.docs.get(region.id)

        docs_region1 = test_mp.backend.regions.docs.set(
            region.id, "Description of test region"
        )

        assert test_mp.backend.regions.docs.get(region.id) == docs_region1

        docs_region2 = test_mp.backend.regions.docs.set(
            region.id, "Different description of test region"
        )

        assert test_mp.backend.regions.docs.get(region.id) == docs_region2

    def test_delete_regiondocs(self, test_mp, request):
        test_mp = request.getfixturevalue(test_mp)
        region = test_mp.backend.regions.create("Region", "Hierarchy")
        docs_region = test_mp.backend.regions.docs.set(
            region.id, "Description of test region"
        )

        assert test_mp.backend.regions.docs.get(region.id) == docs_region

        test_mp.backend.regions.docs.delete(region.id)

        with pytest.raises(Docs.NotFound):
            test_mp.backend.regions.docs.get(region.id)

    def test_get_and_set_scenariodocs(self, test_mp, request):
        test_mp = request.getfixturevalue(test_mp)
        scenario = test_mp.backend.scenarios.create("Scenario")
        docs_scenario = test_mp.backend.scenarios.docs.set(
            scenario.id, "Description of Scenario"
        )
        docs_scenario1 = test_mp.backend.scenarios.docs.get(scenario.id)
        assert docs_scenario == docs_scenario1

    def test_change_empty_scenariodocs(self, test_mp, request):
        test_mp = request.getfixturevalue(test_mp)
        scenario = test_mp.backend.scenarios.create("Scenario")

        with pytest.raises(Docs.NotFound):
            test_mp.backend.scenarios.docs.get(scenario.id)

        docs_scenario1 = test_mp.backend.scenarios.docs.set(
            scenario.id, "Description of test Scenario"
        )

        assert test_mp.backend.scenarios.docs.get(scenario.id) == docs_scenario1

        docs_scenario2 = test_mp.backend.scenarios.docs.set(
            scenario.id, "Different description of test Scenario"
        )

        assert test_mp.backend.scenarios.docs.get(scenario.id) == docs_scenario2

    def test_delete_scenariodocs(self, test_mp, request):
        test_mp = request.getfixturevalue(test_mp)
        scenario = test_mp.backend.scenarios.create("Scenario")
        docs_scenario = test_mp.backend.scenarios.docs.set(
            scenario.id, "Description of test Scenario"
        )

        assert test_mp.backend.scenarios.docs.get(scenario.id) == docs_scenario

        test_mp.backend.scenarios.docs.delete(scenario.id)

        with pytest.raises(Docs.NotFound):
            test_mp.backend.scenarios.docs.get(scenario.id)

    def test_get_and_set_unitdocs(self, test_mp, request):
        test_mp = request.getfixturevalue(test_mp)
        unit = test_mp.backend.units.create("Unit")
        docs_unit = test_mp.backend.units.docs.set(unit.id, "Description of test Unit")
        docs_unit1 = test_mp.backend.units.docs.get(unit.id)

        assert docs_unit == docs_unit1

    def test_change_empty_unitdocs(self, test_mp, request):
        test_mp = request.getfixturevalue(test_mp)
        unit = test_mp.backend.units.create("Unit")

        with pytest.raises(Docs.NotFound):
            test_mp.backend.units.docs.get(unit.id)

        docs_unit1 = test_mp.backend.units.docs.set(unit.id, "Description of test Unit")

        assert test_mp.backend.units.docs.get(unit.id) == docs_unit1

        docs_unit2 = test_mp.backend.units.docs.set(
            unit.id, "Different description of test Unit"
        )

        assert test_mp.backend.units.docs.get(unit.id) == docs_unit2

    def test_delete_unitdocs(self, test_mp, request):
        test_mp = request.getfixturevalue(test_mp)
        unit = test_mp.backend.units.create("Unit")
        docs_unit = test_mp.backend.units.docs.set(unit.id, "Description of test Unit")

        assert test_mp.backend.units.docs.get(unit.id) == docs_unit

        test_mp.backend.units.docs.delete(unit.id)

        with pytest.raises(Docs.NotFound):
            test_mp.backend.units.docs.get(unit.id)

    def test_get_and_set_variabledocs(self, test_mp, request):
        test_mp = request.getfixturevalue(test_mp)
        variable = test_mp.backend.iamc.variables.create("Variable")
        docs_variable = test_mp.backend.iamc.variables.docs.set(
            variable.id, "Description of test Variable"
        )
        docs_variables1 = test_mp.backend.iamc.variables.docs.get(variable.id)

        assert docs_variable == docs_variables1

    def test_change_empty_variabledocs(self, test_mp, request):
        test_mp = request.getfixturevalue(test_mp)
        variable = test_mp.backend.iamc.variables.create("Variable")

        with pytest.raises(Docs.NotFound):
            test_mp.backend.iamc.variables.docs.get(variable.id)

        docs_variable1 = test_mp.backend.iamc.variables.docs.set(
            variable.id, "Description of test Variable"
        )

        assert test_mp.backend.iamc.variables.docs.get(variable.id) == docs_variable1

        docs_variable2 = test_mp.backend.iamc.variables.docs.set(
            variable.id, "Different description of test Variable"
        )

        assert test_mp.backend.iamc.variables.docs.get(variable.id) == docs_variable2

    def test_delete_variabledocs(self, test_mp, request):
        test_mp = request.getfixturevalue(test_mp)
        variable = test_mp.backend.iamc.variables.create("Variable")
        docs_variable = test_mp.backend.iamc.variables.docs.set(
            variable.id, "Description of test Variable"
        )

        assert test_mp.backend.iamc.variables.docs.get(variable.id) == docs_variable

        test_mp.backend.iamc.variables.docs.delete(variable.id)

        with pytest.raises(Docs.NotFound):
            test_mp.backend.iamc.variables.docs.get(variable.id)

    def test_get_and_set_indexsetdocs(self, test_mp, request):
        test_mp = request.getfixturevalue(test_mp)
        run = test_mp.backend.runs.create("Model", "Scenario")
        indexset = test_mp.backend.optimization.indexsets.create(
            run_id=run.id, name="IndexSet"
        )
        docs_indexset = test_mp.backend.optimization.indexsets.docs.set(
            indexset.id, "Description of test IndexSet"
        )
        docs_indexset1 = test_mp.backend.optimization.indexsets.docs.get(indexset.id)

        assert docs_indexset == docs_indexset1

    def test_change_empty_indexsetdocs(self, test_mp, request):
        test_mp = request.getfixturevalue(test_mp)
        run = test_mp.backend.runs.create("Model", "Scenario")
        indexset = test_mp.backend.optimization.indexsets.create(
            run_id=run.id, name="IndexSet"
        )

        with pytest.raises(Docs.NotFound):
            test_mp.backend.optimization.indexsets.docs.get(indexset.id)

        docs_indexset1 = test_mp.backend.optimization.indexsets.docs.set(
            indexset.id, "Description of test IndexSet"
        )

        assert (
            test_mp.backend.optimization.indexsets.docs.get(indexset.id)
            == docs_indexset1
        )

        docs_indexset2 = test_mp.backend.optimization.indexsets.docs.set(
            indexset.id, "Different description of test IndexSet"
        )

        assert (
            test_mp.backend.optimization.indexsets.docs.get(indexset.id)
            == docs_indexset2
        )

    def test_delete_indexsetdocs(self, test_mp, request):
        test_mp = request.getfixturevalue(test_mp)
        run = test_mp.backend.runs.create("Model", "Scenario")
        indexset = test_mp.backend.optimization.indexsets.create(
            run_id=run.id, name="IndexSet"
        )
        docs_indexset = test_mp.backend.optimization.indexsets.docs.set(
            indexset.id, "Description of test IndexSet"
        )

        assert (
            test_mp.backend.optimization.indexsets.docs.get(indexset.id)
            == docs_indexset
        )

        test_mp.backend.optimization.indexsets.docs.delete(indexset.id)

        with pytest.raises(Docs.NotFound):
            test_mp.backend.optimization.indexsets.docs.get(indexset.id)

    def test_get_and_set_scalardocs(self, test_mp, request):
        test_mp = request.getfixturevalue(test_mp)
        run = test_mp.backend.runs.create("Model", "Scenario")
        unit = test_mp.backend.units.create("Unit")
        scalar = test_mp.backend.optimization.scalars.create(
            run_id=run.id, name="Scalar", value=1, unit_name=unit.name
        )
        docs_scalar = test_mp.backend.optimization.scalars.docs.set(
            scalar.id, "Description of test Scalar"
        )
        docs_scalar1 = test_mp.backend.optimization.scalars.docs.get(scalar.id)

        assert docs_scalar == docs_scalar1

    def test_change_empty_scalardocs(self, test_mp, request):
        test_mp = request.getfixturevalue(test_mp)
        run = test_mp.backend.runs.create("Model", "Scenario")
        unit = test_mp.backend.units.create("Unit")
        scalar = test_mp.backend.optimization.scalars.create(
            run_id=run.id, name="Scalar", value=2, unit_name=unit.name
        )

        with pytest.raises(Docs.NotFound):
            test_mp.backend.optimization.scalars.docs.get(scalar.id)

        docs_scalar1 = test_mp.backend.optimization.scalars.docs.set(
            scalar.id, "Description of test Scalar"
        )

        assert test_mp.backend.optimization.scalars.docs.get(scalar.id) == docs_scalar1

        docs_scalar2 = test_mp.backend.optimization.scalars.docs.set(
            scalar.id, "Different description of test Scalar"
        )

        assert test_mp.backend.optimization.scalars.docs.get(scalar.id) == docs_scalar2

    def test_delete_scalardocs(self, test_mp, request):
        test_mp = request.getfixturevalue(test_mp)
        run = test_mp.backend.runs.create("Model", "Scenario")
        unit = test_mp.backend.units.create("Unit")
        scalar = test_mp.backend.optimization.scalars.create(
            run_id=run.id, name="Scalar", value=3, unit_name=unit.name
        )
        docs_scalar = test_mp.backend.optimization.scalars.docs.set(
            scalar.id, "Description of test Scalar"
        )

        assert test_mp.backend.optimization.scalars.docs.get(scalar.id) == docs_scalar

        test_mp.backend.optimization.scalars.docs.delete(scalar.id)

        with pytest.raises(Docs.NotFound):
            test_mp.backend.optimization.scalars.docs.get(scalar.id)
