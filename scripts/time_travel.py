import asyncio
import sys
from dotenv import load_dotenv
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.types import Command

load_dotenv()

from src.config import get_database_url
from src.graph import build_graph

DB_URI = get_database_url()


async def main():
    thread_id = sys.argv[1] if len(sys.argv) > 1 else "incident-101"
    config = {"configurable": {"thread_id": thread_id}}

    async with AsyncPostgresSaver.from_conn_string(DB_URI) as checkpointer:
        graph = build_graph(checkpointer)

        print(f"=== CHECKPOINT HISTORY FOR {thread_id} ===")
        history = [snap async for snap in graph.aget_state_history(config)]
        for i, snapshot in enumerate(history):
            step = snapshot.metadata.get("step")
            next_nodes = snapshot.next
            conf = snapshot.values.get("confidence_score", "N/A")
            print(f"[{i}] Step {step} | Next: {next_nodes} | Confidence: {conf}")

        target_checkpoint = next(
            (snap for snap in history if snap.next == ("remediate",)), None
        )
        if not target_checkpoint:
            print("No 'remediate' checkpoint found. Run scripts/run_incident.py first.")
            return

        cp_id = target_checkpoint.config["configurable"]["checkpoint_id"]
        print(f"\nRewinding to checkpoint_id: {cp_id}")

        forked_config = await graph.aupdate_state(
            target_checkpoint.config,
            {"proposed_command": "kubectl scale deployment checkout-api --replicas=10"},
        )

        print("Replaying from forked state with auto-approval...")
        async for event in graph.astream(
            Command(resume="yes"), forked_config, stream_mode="updates"
        ):
            print("Replay Update:", event)


def _run() -> None:
    # psycopg's async driver cannot use Windows' default ProactorEventLoop.
    if sys.platform == "win32" and sys.version_info >= (3, 12):
        asyncio.run(main(), loop_factory=asyncio.SelectorEventLoop)
    elif sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(main())
    else:
        asyncio.run(main())


if __name__ == "__main__":
    _run()
