from contextlib import suppress
from sqlalchemy import sql


def is_joined(exc: sql.Select, model):
    """Returns `True` if `model` has been joined in `exc`."""
    for visitor in sql.visitors.iterate(exc):
        # Checking for `.join(Child)` clauses
        if visitor.__visit_name__ == "table":
            # Visitor might be of ColumnCollection or so,
            # which cannot be compared to model
            with suppress(TypeError):
                if model == visitor.entity_namespace:  # type: ignore
                    return True
    return False
