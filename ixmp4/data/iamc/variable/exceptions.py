from ixmp4.base_exceptions import DeletionPrevented, NotFound, NotUnique, registry


@registry.register(name="IamcVariableNotFound")
class VariableNotFound(NotFound):
    pass


@registry.register(name="IamcVariableNotUnique")
class VariableNotUnique(NotUnique):
    pass


@registry.register(name="IamcVariableDeletionPrevented")
class VariableDeletionPrevented(DeletionPrevented):
    pass
