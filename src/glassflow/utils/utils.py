import json
import re
from dataclasses import Field, dataclass, fields, is_dataclass, make_dataclass
from datetime import datetime
from decimal import Decimal
from email.message import Message
from enum import Enum
from typing import (Any, Callable, Dict, List, Tuple, Union, get_args,
                    get_origin)
from xmlrpc.client import boolean

from dataclasses_json import DataClassJsonMixin
from typing_inspect import is_optional_type


def generate_url(
    clazz: type,
    server_url: str,
    path: str,
    path_params: dataclass,
    gbls: Dict[str, Dict[str, Dict[str, Any]]] = None,
) -> str:
    path_param_fields: Tuple[Field, ...] = fields(clazz)
    for field in path_param_fields:
        request_metadata = field.metadata.get("request")
        if request_metadata is not None:
            continue

        param_metadata = field.metadata.get("path_param")
        if param_metadata is None:
            continue

        param = getattr(path_params, field.name) if path_params is not None else None
        param = _populate_from_globals(field.name, param, "pathParam", gbls)

        if param is None:
            continue

        f_name = param_metadata.get("field_name", field.name)
        serialization = param_metadata.get("serialization", "")
        if serialization != "":
            serialized_params = _get_serialized_params(
                param_metadata, field.type, f_name, param
            )
            for key, value in serialized_params.items():
                path = path.replace("{" + key + "}", value, 1)
        else:
            if param_metadata.get("style", "simple") == "simple":
                if isinstance(param, List):
                    pp_vals: List[str] = []
                    for pp_val in param:
                        if pp_val is None:
                            continue
                        pp_vals.append(_val_to_string(pp_val))
                    path = path.replace(
                        "{" + param_metadata.get("field_name", field.name) + "}",
                        ",".join(pp_vals),
                        1,
                    )
                elif isinstance(param, Dict):
                    pp_vals: List[str] = []
                    for pp_key in param:
                        if param[pp_key] is None:
                            continue
                        if param_metadata.get("explode"):
                            pp_vals.append(f"{pp_key}={_val_to_string(param[pp_key])}")
                        else:
                            pp_vals.append(f"{pp_key},{_val_to_string(param[pp_key])}")
                    path = path.replace(
                        "{" + param_metadata.get("field_name", field.name) + "}",
                        ",".join(pp_vals),
                        1,
                    )
                elif not isinstance(param, (str, int, float, complex, bool, Decimal)):
                    pp_vals: List[str] = []
                    param_fields: Tuple[Field, ...] = fields(param)
                    for param_field in param_fields:
                        param_value_metadata = param_field.metadata.get("path_param")
                        if not param_value_metadata:
                            continue

                        parm_name = param_value_metadata.get("field_name", field.name)

                        param_field_val = getattr(param, param_field.name)
                        if param_field_val is None:
                            continue
                        if param_metadata.get("explode"):
                            pp_vals.append(
                                f"{parm_name}={_val_to_string(param_field_val)}"
                            )
                        else:
                            pp_vals.append(
                                f"{parm_name},{_val_to_string(param_field_val)}"
                            )
                    path = path.replace(
                        "{" + param_metadata.get("field_name", field.name) + "}",
                        ",".join(pp_vals),
                        1,
                    )
                else:
                    path = path.replace(
                        "{" + param_metadata.get("field_name", field.name) + "}",
                        _val_to_string(param),
                        1,
                    )

    return remove_suffix(server_url, "/") + path


def is_optional(field):
    return get_origin(field) is Union and type(None) in get_args(field)


def get_query_params(
    clazz: type,
    query_params: dataclass,
    gbls: Dict[str, Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, List[str]]:
    params: Dict[str, List[str]] = {}

    param_fields: Tuple[Field, ...] = fields(clazz)
    for field in param_fields:
        request_metadata = field.metadata.get("request")
        if request_metadata is not None:
            continue

        metadata = field.metadata.get("query_param")
        if not metadata:
            continue

        param_name = field.name
        value = getattr(query_params, param_name) if query_params is not None else None

        value = _populate_from_globals(param_name, value, "queryParam", gbls)

        f_name = metadata.get("field_name")
        serialization = metadata.get("serialization", "")
        if serialization != "":
            serialized_parms = _get_serialized_params(
                metadata, field.type, f_name, value
            )
            for key, value in serialized_parms.items():
                if key in params:
                    params[key].extend(value)
                else:
                    params[key] = [value]
        else:
            style = metadata.get("style", "form")
            if style == "deepObject":
                params = {
                    **params,
                    **_get_deep_object_query_params(metadata, f_name, value),
                }
            elif style == "form":
                params = {
                    **params,
                    **_get_delimited_query_params(metadata, f_name, value, ","),
                }
            elif style == "pipeDelimited":
                params = {
                    **params,
                    **_get_delimited_query_params(metadata, f_name, value, "|"),
                }
            else:
                raise Exception("not yet implemented")
    return params


def get_req_specific_headers(headers_params: dataclass) -> Dict[str, str]:
    if headers_params is None:
        return {}

    headers: Dict[str, str] = {}

    param_fields: Tuple[Field, ...] = fields(headers_params)
    for field in param_fields:
        metadata = field.metadata.get("header")
        if not metadata:
            continue

        value = _serialize_header(
            metadata.get("explode", False), getattr(headers_params, field.name)
        )

        if value != "":
            headers[metadata.get("field_name", field.name)] = value

    return headers


def _get_serialized_params(
    metadata: Dict, field_type: type, field_name: str, obj: any
) -> Dict[str, str]:
    params: Dict[str, str] = {}

    serialization = metadata.get("serialization", "")
    if serialization == "json":
        params[metadata.get("field_name", field_name)] = marshal_json(obj, field_type)

    return params


def _get_deep_object_query_params(
    metadata: Dict, field_name: str, obj: any
) -> Dict[str, List[str]]:
    params: Dict[str, List[str]] = {}

    if obj is None:
        return params

    if is_dataclass(obj):
        obj_fields: Tuple[Field, ...] = fields(obj)
        for obj_field in obj_fields:
            obj_param_metadata = obj_field.metadata.get("query_param")
            if not obj_param_metadata:
                continue

            obj_val = getattr(obj, obj_field.name)
            if obj_val is None:
                continue

            if isinstance(obj_val, List):
                for val in obj_val:
                    if val is None:
                        continue

                    if (
                        params.get(
                            f'{metadata.get("field_name", field_name)}[{obj_param_metadata.get("field_name", obj_field.name)}]'
                        )
                        is None
                    ):
                        params[
                            f'{metadata.get("field_name", field_name)}[{obj_param_metadata.get("field_name", obj_field.name)}]'
                        ] = []

                    params[
                        f'{metadata.get("field_name", field_name)}[{obj_param_metadata.get("field_name", obj_field.name)}]'
                    ].append(_val_to_string(val))
            else:
                params[
                    f'{metadata.get("field_name", field_name)}[{obj_param_metadata.get("field_name", obj_field.name)}]'
                ] = [_val_to_string(obj_val)]
    elif isinstance(obj, Dict):
        for key, value in obj.items():
            if value is None:
                continue

            if isinstance(value, List):
                for val in value:
                    if val is None:
                        continue

                    if (
                        params.get(f'{metadata.get("field_name", field_name)}[{key}]')
                        is None
                    ):
                        params[f'{metadata.get("field_name", field_name)}[{key}]'] = []

                    params[f'{metadata.get("field_name", field_name)}[{key}]'].append(
                        _val_to_string(val)
                    )
            else:
                params[f'{metadata.get("field_name", field_name)}[{key}]'] = [
                    _val_to_string(value)
                ]
    return params


def _get_query_param_field_name(obj_field: Field) -> str:
    obj_param_metadata = obj_field.metadata.get("query_param")

    if not obj_param_metadata:
        return ""

    return obj_param_metadata.get("field_name", obj_field.name)


def _get_delimited_query_params(
    metadata: Dict, field_name: str, obj: any, delimiter: str
) -> Dict[str, List[str]]:
    return _populate_form(
        field_name,
        metadata.get("explode", True),
        obj,
        _get_query_param_field_name,
        delimiter,
    )


SERIALIZATION_METHOD_TO_CONTENT_TYPE = {
    "json": "application/json",
    "form": "application/x-www-form-urlencoded",
    "multipart": "multipart/form-data",
    "raw": "application/octet-stream",
    "string": "text/plain",
}


def serialize_request_body(
    request: dataclass,
    request_type: type,
    request_field_name: str,
    nullable: bool,
    optional: bool,
    serialization_method: str,
    encoder=None,
) -> Tuple[str, any, any]:
    if request is None:
        if not nullable and optional:
            return None, None, None

    if not is_dataclass(request) or not hasattr(request, request_field_name):
        return serialize_content_type(
            request_field_name,
            request_type,
            SERIALIZATION_METHOD_TO_CONTENT_TYPE[serialization_method],
            request,
            encoder,
        )

    request_val = getattr(request, request_field_name)

    if request_val is None:
        if not nullable and optional:
            return None, None, None

    request_fields: Tuple[Field, ...] = fields(request)
    request_metadata = None

    for field in request_fields:
        if field.name == request_field_name:
            request_metadata = field.metadata.get("request")
            break

    if request_metadata is None:
        raise Exception("invalid request type")

    return serialize_content_type(
        request_field_name,
        request_type,
        request_metadata.get("media_type", "application/octet-stream"),
        request_val,
    )


def serialize_content_type(
    field_name: str,
    request_type: any,
    media_type: str,
    request: dataclass,
    encoder=None,
) -> Tuple[str, any, List[List[any]]]:
    if re.match(r"(application|text)\/.*?\+*json.*", media_type) is not None:
        return media_type, marshal_json(request, request_type, encoder), None
    if re.match(r"multipart\/.*", media_type) is not None:
        return serialize_multipart_form(media_type, request)
    if re.match(r"application\/x-www-form-urlencoded.*", media_type) is not None:
        return media_type, serialize_form_data(field_name, request), None
    if isinstance(request, (bytes, bytearray)):
        return media_type, request, None
    if isinstance(request, str):
        return media_type, request, None

    raise Exception(
        f"invalid request body type {type(request)} for mediaType {media_type}"
    )


def serialize_multipart_form(
    media_type: str, request: dataclass
) -> Tuple[str, any, List[List[any]]]:
    form: List[List[any]] = []
    request_fields = fields(request)

    for field in request_fields:
        val = getattr(request, field.name)
        if val is None:
            continue

        field_metadata = field.metadata.get("multipart_form")
        if not field_metadata:
            continue

        if field_metadata.get("file") is True:
            file_fields = fields(val)

            file_name = ""
            field_name = ""
            content = bytes()

            for file_field in file_fields:
                file_metadata = file_field.metadata.get("multipart_form")
                if file_metadata is None:
                    continue

                if file_metadata.get("content") is True:
                    content = getattr(val, file_field.name)
                else:
                    field_name = file_metadata.get("field_name", file_field.name)
                    file_name = getattr(val, file_field.name)
            if field_name == "" or file_name == "" or content == bytes():
                raise Exception("invalid multipart/form-data file")

            form.append([field_name, [file_name, content]])
        elif field_metadata.get("json") is True:
            to_append = [
                field_metadata.get("field_name", field.name),
                [None, marshal_json(val, field.type), "application/json"],
            ]
            form.append(to_append)
        else:
            field_name = field_metadata.get("field_name", field.name)
            if isinstance(val, List):
                for value in val:
                    if value is None:
                        continue
                    form.append([field_name + "[]", [None, _val_to_string(value)]])
            else:
                form.append([field_name, [None, _val_to_string(val)]])
    return media_type, None, form


def serialize_form_data(field_name: str, data: dataclass) -> Dict[str, any]:
    form: Dict[str, List[str]] = {}

    if is_dataclass(data):
        for field in fields(data):
            val = getattr(data, field.name)
            if val is None:
                continue

            metadata = field.metadata.get("form")
            if metadata is None:
                continue

            field_name = metadata.get("field_name", field.name)

            if metadata.get("json"):
                form[field_name] = [marshal_json(val, field.type)]
            else:
                if metadata.get("style", "form") == "form":
                    form = {
                        **form,
                        **_populate_form(
                            field_name,
                            metadata.get("explode", True),
                            val,
                            _get_form_field_name,
                            ",",
                        ),
                    }
                else:
                    raise Exception(f"Invalid form style for field {field.name}")
    elif isinstance(data, Dict):
        for key, value in data.items():
            form[key] = [_val_to_string(value)]
    else:
        raise Exception(f"Invalid request body type for field {field_name}")

    return form


def _get_form_field_name(obj_field: Field) -> str:
    obj_param_metadata = obj_field.metadata.get("form")

    if not obj_param_metadata:
        return ""

    return obj_param_metadata.get("field_name", obj_field.name)


def _populate_form(
    field_name: str,
    explode: boolean,
    obj: any,
    get_field_name_func: Callable,
    delimiter: str,
) -> Dict[str, List[str]]:
    params: Dict[str, List[str]] = {}

    if obj is None:
        return params

    if is_dataclass(obj):
        items = []

        obj_fields: Tuple[Field, ...] = fields(obj)
        for obj_field in obj_fields:
            obj_field_name = get_field_name_func(obj_field)
            if obj_field_name == "":
                continue

            val = getattr(obj, obj_field.name)
            if val is None:
                continue

            if explode:
                params[obj_field_name] = [_val_to_string(val)]
            else:
                items.append(f"{obj_field_name}{delimiter}{_val_to_string(val)}")

        if len(items) > 0:
            params[field_name] = [delimiter.join(items)]
    elif isinstance(obj, Dict):
        items = []
        for key, value in obj.items():
            if value is None:
                continue

            if explode:
                params[key] = _val_to_string(value)
            else:
                items.append(f"{key}{delimiter}{_val_to_string(value)}")

        if len(items) > 0:
            params[field_name] = [delimiter.join(items)]
    elif isinstance(obj, List):
        items = []

        for value in obj:
            if value is None:
                continue

            if explode:
                if field_name not in params:
                    params[field_name] = []
                params[field_name].append(_val_to_string(value))
            else:
                items.append(_val_to_string(value))

        if len(items) > 0:
            params[field_name] = [delimiter.join([str(item) for item in items])]
    else:
        params[field_name] = [_val_to_string(obj)]

    return params


def _serialize_header(explode: bool, obj: any) -> str:
    if obj is None:
        return ""

    if is_dataclass(obj):
        items = []
        obj_fields: Tuple[Field, ...] = fields(obj)
        for obj_field in obj_fields:
            obj_param_metadata = obj_field.metadata.get("header")

            if not obj_param_metadata:
                continue

            obj_field_name = obj_param_metadata.get("field_name", obj_field.name)
            if obj_field_name == "":
                continue

            val = getattr(obj, obj_field.name)
            if val is None:
                continue

            if explode:
                items.append(f"{obj_field_name}={_val_to_string(val)}")
            else:
                items.append(obj_field_name)
                items.append(_val_to_string(val))

        if len(items) > 0:
            return ",".join(items)
    elif isinstance(obj, Dict):
        items = []

        for key, value in obj.items():
            if value is None:
                continue

            if explode:
                items.append(f"{key}={_val_to_string(value)}")
            else:
                items.append(key)
                items.append(_val_to_string(value))

        if len(items) > 0:
            return ",".join([str(item) for item in items])
    elif isinstance(obj, List):
        items = []

        for value in obj:
            if value is None:
                continue

            items.append(_val_to_string(value))

        if len(items) > 0:
            return ",".join(items)
    else:
        return f"{_val_to_string(obj)}"

    return ""


def unmarshal_json(data, typ, decoder=None):
    unmarshal = make_dataclass("Unmarshal", [("res", typ)], bases=(DataClassJsonMixin,))
    json_dict = json.loads(data)
    try:
        out = unmarshal.from_dict({"res": json_dict})
    except AttributeError as attr_err:
        raise AttributeError(
            f"unable to unmarshal {data} as {typ} - {attr_err}"
        ) from attr_err

    return out.res if decoder is None else decoder(out.res)


def marshal_json(val, typ, encoder=None):
    if not is_optional_type(typ) and val is None:
        raise ValueError(f"Could not marshal None into non-optional type: {typ}")

    marshal = make_dataclass("Marshal", [("res", typ)], bases=(DataClassJsonMixin,))
    marshaller = marshal(res=val)
    json_dict = marshaller.to_dict()
    val = json_dict["res"] if encoder is None else encoder(json_dict["res"])

    return json.dumps(val, separators=(",", ":"), sort_keys=True)


def match_content_type(content_type: str, pattern: str) -> boolean:
    if pattern in (content_type, "*", "*/*"):
        return True

    msg = Message()
    msg["content-type"] = content_type
    media_type = msg.get_content_type()

    if media_type == pattern:
        return True

    parts = media_type.split("/")
    if len(parts) == 2:
        if pattern in (f"{parts[0]}/*", f"*/{parts[1]}"):
            return True

    return False


def get_field_name(name):
    def override(_, _field_name=name):
        return _field_name

    return override


def _val_to_string(val):
    if isinstance(val, bool):
        return str(val).lower()
    if isinstance(val, datetime):
        return val.isoformat().replace("+00:00", "Z")
    if isinstance(val, Enum):
        return str(val.value)

    return str(val)


def _populate_from_globals(
    param_name: str,
    value: any,
    param_type: str,
    gbls: Dict[str, Dict[str, Dict[str, Any]]],
):
    if value is None and gbls is not None:
        if "parameters" in gbls:
            if param_type in gbls["parameters"]:
                if param_name in gbls["parameters"][param_type]:
                    global_value = gbls["parameters"][param_type][param_name]
                    if global_value is not None:
                        value = global_value

    return value


def remove_suffix(input_string, suffix):
    if suffix and input_string.endswith(suffix):
        return input_string[: -len(suffix)]
    return input_string
