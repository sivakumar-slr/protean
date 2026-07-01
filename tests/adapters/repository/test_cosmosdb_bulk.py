"""Live Cosmos DB tests for bulk update on the CosmosDB adapter."""

import os
import uuid

import pytest
from azure.cosmos import CosmosClient, PartitionKey, exceptions

from protean import Domain
from protean.adapters.repository.cosmosdb import derive_schema_name
from protean.core.aggregate import BaseAggregate
from protean.fields import String

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.getenv("DATABASE_URI") or not os.getenv("DATABASE_KEY"),
        reason="DATABASE_URI and DATABASE_KEY required for live Cosmos tests",
    ),
]


class BulkProduct(BaseAggregate):
    name = String(max_length=50, required=True)
    category = String(max_length=50, required=True)
    status = String(max_length=20, default="UNREAD")


@pytest.fixture(scope="module")
def cosmos_bulk_domain():
    domain = Domain(__file__, "CosmosBulkTest")
    domain.register(BulkProduct)
    domain.config["databases"] = {
        "default": {
            "provider": "cosmosdb",
            "database_uri": os.environ["DATABASE_URI"],
            "database_key": os.environ["DATABASE_KEY"],
            "database_name": os.getenv(
                "COSMOS_BULK_TEST_DATABASE", "test_cosmos_bulk_db"
            ),
            "bulk_concurrency": 4,
        }
    }
    domain.init(traverse=False)

    client = CosmosClient(os.environ["DATABASE_URI"], os.environ["DATABASE_KEY"])
    database_name = domain.config["databases"]["default"]["database_name"]
    database = client.create_database_if_not_exists(id=database_name)

    with domain.domain_context():
        dao = domain.repository_for(BulkProduct)._dao
        container_name = derive_schema_name(dao.model_cls)
        database.create_container_if_not_exists(
            id=container_name,
            partition_key=PartitionKey(path="/id"),
        )
        yield domain

    try:
        client.delete_database(database_name)
    except exceptions.CosmosHttpResponseError:
        pass


def test_update_all_patches_matching_documents(cosmos_bulk_domain):
    with cosmos_bulk_domain.domain_context():
        repo = cosmos_bulk_domain.repository_for(BulkProduct)
        dao = repo._dao

        for index in range(5):
            repo.add(
                BulkProduct(
                    id=str(uuid.uuid4()),
                    name=f"item-{index}",
                    category="bulk-user-1",
                    status="UNREAD",
                )
            )
        repo.add(
            BulkProduct(
                id=str(uuid.uuid4()),
                name="already-read",
                category="bulk-user-1",
                status="READ",
            )
        )

        updated = dao.query.filter(
            category="bulk-user-1", status="UNREAD"
        ).update_all(status="READ")

        assert updated == 5

        unread = dao.query.filter(category="bulk-user-1", status="UNREAD").all()
        read_items = dao.query.filter(category="bulk-user-1", status="READ").all()
        assert unread.total == 0
        assert read_items.total == 6


def test_update_all_returns_zero_when_nothing_matches(cosmos_bulk_domain):
    with cosmos_bulk_domain.domain_context():
        dao = cosmos_bulk_domain.repository_for(BulkProduct)._dao
        updated = dao.query.filter(category="missing-user").update_all(status="READ")
        assert updated == 0
