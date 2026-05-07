from collections.abc import Iterable, Mapping, Sequence
from typing import Any, Callable

FilterValueTransformer = Callable[[Any], dict[str, Any] | None]
MappingFilterConverter = Callable[[Mapping[str, Any]], Mapping[str, Any]]


def make_str_like_transformer(field: str = "name") -> FilterValueTransformer:
    def transform(value: Any) -> dict[str, Any] | None:
        if isinstance(value, str):
            return {f"{field}__like": value}
        return None

    return transform


def make_iterable_str_in_transformer(field: str = "name") -> FilterValueTransformer:
    def transform(value: Any) -> dict[str, Any] | None:
        if isinstance(value, (str, bytes, Mapping)):
            return None
        if not isinstance(value, Iterable):
            return None

        items = list(value)
        if all(isinstance(item, str) for item in items):
            return {f"{field}__in": items}
        return None

    return transform


def make_mapping_transformer(
    converter: MappingFilterConverter,
) -> FilterValueTransformer:
    def transform(value: Any) -> dict[str, Any] | None:
        if not isinstance(value, Mapping):
            return None
        return dict(converter(value))

    return transform


def rename_facade_filter_key(key: str, *, key_map: Mapping[str, str]) -> str:
    for facade_name, data_name in key_map.items():
        if key.startswith(facade_name):
            return key.replace(facade_name, data_name, 1)
    return key


def convert_facade_filter(
    filter_values: Mapping[str, Any],
    *,
    key_map: Mapping[str, str],
    field_transformers: Mapping[str, Sequence[FilterValueTransformer]],
) -> dict[str, Any]:
    converted: dict[str, Any] = {}
    for key, value in filter_values.items():
        transformed = value
        transformers = field_transformers.get(key)
        if transformers is not None:
            for transformer in transformers:
                transformed_value = transformer(value)
                if transformed_value is not None:
                    transformed = transformed_value
                    break

        converted[rename_facade_filter_key(key, key_map=key_map)] = transformed

    return converted
