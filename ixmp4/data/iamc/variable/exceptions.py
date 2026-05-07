from ixmp4.base_exceptions import DeletionPrevented, NotFound, NotUnique, registry


@registry.register(name="IamcVariableNotFound")
class VariableNotFound(NotFound):
    message = "Variable not found."


@registry.register(name="IamcVariableNotUnique")
class VariableNotUnique(NotUnique):
    message = "Variable is not unique."


@registry.register(name="IamcVariableDeletionPrevented")
class VariableDeletionPrevented(DeletionPrevented):
    message = "Cannot delete variable: it is used in datapoints."
