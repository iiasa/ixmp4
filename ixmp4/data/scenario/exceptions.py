from ixmp4.base_exceptions import DeletionPrevented, NotFound, NotUnique, registry


@registry.register()
class ScenarioNotFound(NotFound):
    pass


@registry.register()
class ScenarioNotUnique(NotUnique):
    pass


@registry.register()
class ScenarioDeletionPrevented(DeletionPrevented):
    pass
