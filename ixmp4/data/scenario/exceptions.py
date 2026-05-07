from ixmp4.base_exceptions import DeletionPrevented, NotFound, NotUnique, registry


@registry.register()
class ScenarioNotFound(NotFound):
    message = "Scenario not found."


@registry.register()
class ScenarioNotUnique(NotUnique):
    message = "Scenario is not unique."


@registry.register()
class ScenarioDeletionPrevented(DeletionPrevented):
    message = "Cannot delete scenario: it is referenced by runs."
