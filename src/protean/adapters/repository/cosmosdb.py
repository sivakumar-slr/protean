"""Module containing repository implementation for CosmosDB"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any
from uuid import UUID

from azure.cosmos import CosmosClient, PartitionKey, exceptions
from protean.core.model import BaseModel
from protean.core.queryset import ResultSet
from protean.port.dao import BaseDAO
from protean.port.provider import BaseProvider
from protean.utils.container import Options
from protean.utils.query import Q
from protean.utils.reflection import attributes


logger = logging.getLogger(__name__)

_BULK_QUERY_BATCH = 100  # Max ids fetched per query round before patching/deleting.
_PATCH_MAX_RETRIES = 3
_TRANSIENT_COSMOS_STATUS = {408, 429, 449, 500, 503}


def _build_patch_ops(values: dict) -> list:
    """Build Cosmos patch ops; use remove (not set null) for None values."""
    ops = []
    for field, value in _jsonify(values).items():
        if value is None:
            ops.append({"op": "remove", "path": f"/{field}"})
        else:
            ops.append({"op": "set", "path": f"/{field}", "value": value})
    return ops


def _jsonify(value: Any) -> Any:
    """Convert patch values to Cosmos-safe JSON (enums, datetimes, UUIDs, etc.)."""
    if isinstance(value, dict):
        return {key: _jsonify(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonify(item) for item in value]
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, Enum):
        return value.value
    return value

def derive_schema_name(model_cls):
    if hasattr(model_cls.meta_, "schema_name") and model_cls.meta_.schema_name:
        return model_cls.meta_.schema_name
    else:
        return model_cls.meta_.part_of.meta_.schema_name


class CosmosDBModel(BaseModel):
    """A model for the dictionary repository"""

    @classmethod
    def from_entity(cls, entity) -> "CosmosDBModel":
        """Convert the entity to a dictionary record"""
        dict_obj = {}
        for attribute_name in attributes(entity):
            value = getattr(entity, attribute_name)
            if isinstance(value, datetime):
                value = value.isoformat()
            dict_obj[attribute_name] = value
        return dict_obj

    @classmethod
    def to_entity(cls, item: "CosmosDBModel"):
        """Convert the dictionary record to an entity"""
        keys_to_remove = {"_rid", "_ts", "_etag", "_self", "_attachments"}
        item = {k: v for k, v in item.items() if k not in keys_to_remove}
        return cls.meta_.part_of(**item)

class CosmosDBProvider(BaseProvider):
    __database__ = "cosmosdb"

    def __init__(self, name, domain, conn_info: dict):
        """Initialize Provider with Connection/Adapter details"""
        self.client = None
        self.database = None
        self.container = None

        conn_info["database_uri"] = conn_info["database_uri"]
        conn_info["database_key"] = conn_info["database_key"]
        conn_info["database_name"] = conn_info["database_name"]

        super().__init__(name, domain, conn_info)

        # A temporary cache of already constructed model classes
        self._model_classes = {}
        # Parallel patch/delete workers; set bulk_concurrency in database config if needed.
        self._bulk_concurrency = int(conn_info.get("bulk_concurrency", 16))

    def get_connection(self):
        """Get the connection object for the repository"""
        conn = CosmosClient(self.conn_info["database_uri"], self.conn_info["database_key"])
        self.client = conn.get_database_client(self.conn_info["database_name"])
        return self.client
    
    def raw(self, query: Any, data: Any = None):
        """Run raw query directly on the database

        Query should be executed immediately on the database as a separate unit of work
            (in a different transaction context). The results should be returned as returned by
            the database without any intervention. It is left to the consumer to interpret and
            organize the results correctly.
        """
        raise NotImplementedError

    def get_session(self):
        """Establish a new session with the database.

        Typically the session factory should be created once per application. Which is then
        held on to and passed to different transactions.

        In Protean's case, the session scope and the transaction scope match. Which means that a
        new session is created when a transaction needs to be initiated (at the beginning of
        request handling, for example) and terminated (after committing or rolling back) at the end
        of the process. The session will be used as a component in Unit of Work Pattern, to handle
        transactions reliably.

        Sessions are made available to requests as part of a Context Manager.
        """
        return CosmosDBSession(self)

    def decorate_model_class(self, entity_cls, model_cls):
        schema_name = derive_schema_name(model_cls)
        model_cls.meta_.container_name = schema_name
        model_cls.meta_.partition_key = PartitionKey(path="/id")

        # Return the model class if it was already seen/decorated
        if schema_name in self._model_classes:
            return self._model_classes[schema_name]

        # If `model_cls` is already subclassed from MemoryModel,
        #   this method call is a no-op
        if issubclass(model_cls, CosmosDBModel):
            return model_cls
        else:
            custom_attrs = {
                key: value
                for (key, value) in vars(model_cls).items()
                if key not in ["Meta", "__module__", "__doc__", "__weakref__"]
            }

            meta_ = Options()
            meta_.part_of = entity_cls

            custom_attrs.update({"meta_": meta_})
            # FIXME Ensure the custom model attributes are constructed properly
            decorated_model_cls = type(
                model_cls.__name__, (CosmosDBModel, model_cls), custom_attrs
            )

            # Memoize the constructed model class
            self._model_classes[schema_name] = decorated_model_cls

            return decorated_model_cls

    def construct_model_class(self, entity_cls):
        """Return associated, fully-baked Model class"""
        model_cls = None

        # Return the model class if it was already seen/decorated
        if entity_cls.meta_.schema_name in self._model_classes:
            model_cls = self._model_classes[entity_cls.meta_.schema_name]
        else:
            meta_ = Options()
            meta_.part_of = entity_cls

            attrs = {
                "meta_": meta_,
            }
            # FIXME Ensure the custom model attributes are constructed properly
            model_cls = type(entity_cls.__name__ + "Model", (CosmosDBModel,), attrs)

            # Memoize the constructed model class
            self._model_classes[entity_cls.meta_.schema_name] = model_cls

        # Set Entity Class as a class level attribute for the Model, to be able to reference later.
        return model_cls

    def is_alive(self) -> bool:
        """Check if the connection is alive"""
        try:
            conn = self.get_connection()
            conn.read()
            return True
        except Exception as e:
            return False

    def get_dao(self, entity_cls, model_cls):
        """Return a DAO object configured with a live connection"""
        return CosmosDBDAO(self.domain, self, entity_cls, model_cls)

    def close(self):
        """Close connection to CosmosDB."""
        self.client = None
    
    def construct_schema(self, *args, **kwargs):
        """Define schema creation logic for CosmosDB."""
        if not self.database or not self.container:
            self.get_connection()
        
        if self.container_name not in [c["id"] for c in self.database.list_containers()]:
            self.database.create_container(id=self.container_name, partition_key=kwargs.get("partition_key", None))
    
    def cleanup(self):
        """Handle cleanup activities like closing connections."""
        self.close()

class CosmosDBDAO(BaseDAO):
    def __repr__(self) -> str:
        return f"CosmosDBDAO <{self.entity_cls.__name__}>"

    def _create(self, model_obj):
        """Add a new entity to CosmosDB."""
        conn = self.provider.get_connection()
        try:
            schema_name = derive_schema_name(self.model_cls)
            container_client = conn.get_container_client(schema_name)
            container_client.create_item(body=model_obj)
        except Exception as exc:
            logger.error(f"Error while creating: {exc}")
            raise
        return model_obj
    
    def _update(self, model_obj):
        """Update an existing entity."""
        conn = self.provider.get_connection()
        try:
            schema_name = derive_schema_name(self.model_cls)
            container_client = conn.get_container_client(schema_name)
            container_client.replace_item(item=model_obj["id"], body=model_obj)
        except Exception as exc:
            logger.error(f"Error while creating: {exc}")
            raise
        return model_obj
    
    def _delete(self, model_obj):
        """Delete an entity."""
        conn = self.provider.get_connection()
        try:
            schema_name = derive_schema_name(self.model_cls)
            container_client = conn.get_container_client(schema_name)
            container_client.delete_item(item=model_obj["id"], partition_key=model_obj.get("partition_key"))
        except Exception as exc:
            logger.error(f"Error while creating: {exc}")
            raise
        return model_obj

    def _raw(self, query: Any, data: Any = None):
        """Run raw query on Data source.

        Running a raw query on the data store should always returns entity instance objects. If
        the results were not synthesizable back into entity objects, an exception should be thrown.
        """
        raise NotImplementedError

    def _container(self):
        conn = self.provider.get_connection()
        return conn.get_container_client(derive_schema_name(self.model_cls))

    def _query_parameters(self, params):
        return [{"name": key, "value": value} for key, value in params.items()]

    def _matched_keys(self, criteria: Q, limit: int = _BULK_QUERY_BATCH):
        """Return (id, partition_key) pairs matching criteria — ids only, not full documents."""
        if not criteria.children:
            return []

        where_clause, params = self._build_filters(criteria)
        batch_params = dict(params)
        batch_params["@limit"] = limit
        sql = f"SELECT c.id FROM c WHERE {where_clause} OFFSET 0 LIMIT @limit"
        items = list(
            self._container().query_items(
                query=sql,
                parameters=self._query_parameters(batch_params),
                enable_cross_partition_query=True,
            )
        )
        return [(item["id"], item["id"]) for item in items]

    def _bulk_execute(self, keys, op) -> int:
        """Run op(key) concurrently; skip not-found, raise on first other Cosmos error."""
        if not keys:
            return 0

        count = 0
        errors = []
        with ThreadPoolExecutor(max_workers=self.provider._bulk_concurrency) as pool:
            futures = [pool.submit(op, key) for key in keys]
            for future in as_completed(futures):
                try:
                    future.result()
                    count += 1
                except exceptions.CosmosResourceNotFoundError:
                    pass
                except exceptions.CosmosHttpResponseError as exc:
                    errors.append(exc)

        if errors:
            raise errors[0]
        return count

    def _patch_or_replace(self, container, key, values: dict):
        """Patch fields with retries; fall back to read-modify-replace if patch keeps failing."""
        item_id, partition_key = key
        patch_ops = _build_patch_ops(values)
        patch_chunks = [
            patch_ops[index : index + 10] for index in range(0, len(patch_ops), 10)
        ]
        delay = 0.05

        for attempt in range(_PATCH_MAX_RETRIES):
            try:
                for chunk in patch_chunks:
                    container.patch_item(
                        item=item_id,
                        partition_key=partition_key,
                        patch_operations=chunk,
                        no_response=True,
                    )
                return
            except exceptions.CosmosHttpResponseError as exc:
                status = getattr(exc, "status_code", None)
                if status not in _TRANSIENT_COSMOS_STATUS or attempt == _PATCH_MAX_RETRIES - 1:
                    break
                time.sleep(delay)
                delay *= 2

        doc = container.read_item(item=item_id, partition_key=partition_key)
        for field, value in _jsonify(values).items():
            if value is None:
                doc.pop(field, None)
            else:
                doc[field] = value
        container.replace_item(item=item_id, body=doc)

    def _update_all(self, criteria: Q, *args, **kwargs):
        """Bulk patch items matching criteria without loading full documents.

        Uses patch_item (partial update) instead of replace_item. Processes in
        batches of _BULK_QUERY_BATCH until no matching ids remain.
        """
        values = {}
        if args:
            values.update(args[0])
        values.update(kwargs)
        if not values:
            return 0

        container = self._container()
        total_updated = 0

        def patch_key(key):
            self._patch_or_replace(container, key, values)

        while True:
            keys = self._matched_keys(criteria)
            if not keys:
                break
            total_updated += self._bulk_execute(keys, patch_key)
            if len(keys) < _BULK_QUERY_BATCH:
                break

        return total_updated

    def _delete_all(self, criteria: Q = None):
        """Bulk delete items matching criteria in batches of _BULK_QUERY_BATCH."""
        criteria = criteria or Q()
        container = self._container()
        total_deleted = 0

        def delete_key(key):
            container.delete_item(item=key[0], partition_key=key[1])

        while True:
            keys = self._matched_keys(criteria)
            if not keys:
                break
            total_deleted += self._bulk_execute(keys, delete_key)
            if len(keys) < _BULK_QUERY_BATCH:
                break

        return total_deleted

    def _build_filters(self, criteria: Q):
        """Recursively Build the filters from the criteria object into CosmosDB SQL query"""
        query_parts = []
        params = {}
        param_count = 0

        def build_condition(child, negated=False):
            nonlocal param_count
            if isinstance(child, Q):
                nested_query, nested_params = self._build_filters(child)
                return f"({nested_query})", nested_params
            else:
                field, value = child
                param_name = f"@param{param_count}"
                param_count += 1
                params[param_name] = value

                # Handle field lookups (e.g. age__gt, name__contains)
                if "__" in field:
                    field_name, lookup = field.split("__")
                    if lookup == "exact":
                        condition = f"c.{field_name} = {param_name}"
                    elif lookup == "iexact":
                        condition = f"LOWER(c.{field_name}) = LOWER({param_name})"
                    elif lookup == "contains":
                        condition = f"CONTAINS(c.{field_name}, {param_name})"
                    elif lookup == "icontains":
                        condition = f"CONTAINS(LOWER(c.{field_name}), LOWER({param_name}))"
                    elif lookup == "startswith":
                        condition = f"STARTSWITH(c.{field_name}, {param_name})"
                    elif lookup == "endswith":
                        condition = f"ENDSWITH(c.{field_name}, {param_name})"
                    elif lookup == "gt":
                        condition = f"c.{field_name} > {param_name}"
                    elif lookup == "gte":
                        condition = f"c.{field_name} >= {param_name}"
                    elif lookup == "lt":
                        condition = f"c.{field_name} < {param_name}"
                    elif lookup == "lte":
                        condition = f"c.{field_name} <= {param_name}"
                    elif lookup == "in":
                        condition = f"ARRAY_CONTAINS({param_name}, c.{field_name})"
                    else:
                        raise ValueError(f"Unsupported lookup type: {lookup}")
                else:
                    condition = f"c.{field} = {param_name}"

                if negated:
                    condition = f"NOT ({condition})"
                return condition, params

        if criteria.connector == criteria.AND:
            for child in criteria.children:
                condition, child_params = build_condition(child, criteria.negated)
                query_parts.append(condition)
                params.update(child_params)
            query = " AND ".join(query_parts)
        else:  # OR
            for child in criteria.children:
                condition, child_params = build_condition(child, criteria.negated)
                query_parts.append(condition)
                params.update(child_params)
            query = " OR ".join(query_parts)

        # Convert parameters to the format expected by CosmosDB
        cosmos_params = {}
        for i, (k, v) in enumerate(params.items()):
            cosmos_params[f"@param{i}"] = v

        return query, cosmos_params

    def _filter(
        self, criteria: Q, offset: int = 0, limit: int = 10, order_by: list = ()
    ) -> ResultSet:
        """Run raw query on Data source.

        Running a raw query on the data store should always returns entity instance objects. If
        the results were not synthesizable back into entity objects, an exception should be thrown.
        """
        conn = self.provider.get_connection()
        try:
            schema_name = derive_schema_name(self.model_cls)
            container_client = conn.get_container_client(schema_name)

            # Build the query and parameters
            query = "SELECT * FROM c"
            params = {}
            
            if criteria.children:
                where_clause, params = self._build_filters(criteria)
                query = f"{query} WHERE {where_clause}"

            # Add ORDER BY clause if specified
            if order_by:
                order_clauses = []
                for order_col in order_by:
                    col = order_col.lstrip("-")
                    direction = "DESC" if order_col.startswith("-") else "ASC"
                    order_clauses.append(f"c.{col} {direction}")
                query = f"{query} ORDER BY {', '.join(order_clauses)}"

            # Add OFFSET and LIMIT
            query = f"{query} OFFSET {offset} LIMIT {limit}"

            # Execute the query
            items = list(container_client.query_items(
                query=query,
                parameters=[{"name": k, "value": v} for k, v in params.items()],
                enable_cross_partition_query=True
            ))

            # Get total count for pagination
            count_query = "SELECT VALUE COUNT(1) FROM c"
            if criteria.children:
                count_query = f"{count_query} WHERE {where_clause}"
            
            # Convert parameters to the format expected by CosmosDB
            cosmos_params = []
            for i, (k, v) in enumerate(params.items()):
                cosmos_params.append({
                    "name": f"@param{i}",
                    "value": v
                })

            count_result = list(container_client.query_items(
                query=count_query,
                parameters=cosmos_params,
                enable_cross_partition_query=True
            ))
            total = count_result[0] if count_result else 0

            result = ResultSet(
                offset=offset,
                limit=limit,
                total=total,
                items=items,
            )
        except Exception as exc:
            logger.error(f"Error while filtering: {exc}")
            raise
        return result


class CosmosDBSession:
    """A Session wrapper for Cosmosdb Database.

    Cosmosdb does not support Transactions or Sessions, so this class is
    essential a no-op, and acts as a passthrough for all transactions.
    """

    def __init__(self, provider, new_connection=False):
        self._provider = provider
        self.is_active = True

    def add(self, element):
        dao = self._provider.get_dao(element.__class__)
        dao.create(element.to_dict())

    def delete(self, element):
        dao = self._provider.get_dao(element.__class__)
        dao.delete(element)

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        pass
