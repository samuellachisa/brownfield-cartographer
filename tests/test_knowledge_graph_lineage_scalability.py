from src.graph.knowledge_graph import KnowledgeGraph
from src.models import DatasetNode, TransformationNode


def _add_linear_chain(graph: KnowledgeGraph, length: int) -> None:
    """Helper to add a simple linear dataset chain of given length."""
    prev = None
    for i in range(length):
        name = f"ds_{i}"
        graph.add_dataset(DatasetNode(name=name, storage_type="table"))
        if prev is not None:
            t = TransformationNode(
                source_datasets=[prev],
                target_datasets=[name],
                transformation_type="sql",
                source_file="pipeline.sql",
                line_range=(1, 1),
                sql_query_if_applicable=None,
            )
            graph.add_transformation(t)
        prev = name


def test_blast_radius_respects_max_nodes_and_depth() -> None:
    g = KnowledgeGraph()
    _add_linear_chain(g, length=100)

    # Without limits we can reach the tail of the chain.
    full = g.blast_radius("ds_0", direction="downstream")
    assert "ds_99" in full.nodes

    # With a small depth cap we only see a prefix.
    capped_depth = g.blast_radius("ds_0", direction="downstream", max_depth=3)
    assert "ds_4" not in capped_depth.nodes

    # With a node cap we stop once the budget is exhausted.
    capped_nodes = g.blast_radius("ds_0", direction="downstream", max_nodes=5)
    assert len(capped_nodes.nodes) <= 5


def test_lineage_edges_carry_direction_and_sensitivity() -> None:
    g = KnowledgeGraph()
    # Mark one dataset as high-sensitivity and verify it propagates to edges.
    src = DatasetNode(name="pii_users", storage_type="table", sensitivity="high")
    tgt = DatasetNode(name="analytics_users", storage_type="table")
    g.add_dataset(src)
    g.add_dataset(tgt)
    t = TransformationNode(
        source_datasets=[src.name],
        target_datasets=[tgt.name],
        transformation_type="sql",
        source_file="job.sql",
        line_range=(10, 20),
        sql_query_if_applicable="select * from pii_users",
    )
    g.add_transformation(t)

    # Find the edges around the transformation node.
    for u, v, data in g.lineage_graph.edges(data=True):
        if data.get("type") == "CONSUMES":
            assert data.get("direction") == "read"
        if data.get("type") == "PRODUCES":
            assert data.get("direction") == "write"
        if u == src.name or v == src.name:
            # Any edge touching the PII dataset should carry its sensitivity.
            assert data.get("sensitivity") == "high"

