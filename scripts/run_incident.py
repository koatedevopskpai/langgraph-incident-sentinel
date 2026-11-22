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
        await checkpointer.setup()
        graph = build_graph(checkpointer)

        print("--- MERMAID GRAPH ---")
        print(graph.get_graph().draw_mermaid())
        print("---------------------\n")

        existing_state = await graph.aget_state(config)

        if not existing_state.values:
            print(f"[{thread_id}] Starting new async incident investigation...")
            initial_input = {
                "incident_id": thread_id,
                "alert_summary": "PagerDuty: High 502 error rate on checkout-api",
                "findings": [],
                "iteration": 0,
                "guardrail_error": "",
            }
            async for event in graph.astream(
                initial_input, config, stream_mode="updates"
            ):
                print("Update:", event)
        else:
            print(f"[{thread_id}] Resuming existing checkpoint from Postgres...")

        current_state = await graph.aget_state(config)
        if current_state.next:
            interrupt_data = current_state.tasks[0].interrupts[0].value
            print("\n=== HUMAN APPROVAL REQUIRED (Graph Paused) ===")
            for k, v in interrupt_data.items():
                print(f"  {k}: {v}")

            decision = input("\nEnter 'yes', 'no', or a custom CLI command: ").strip()
            async for event in graph.astream(
                Command(resume=decision), config, stream_mode="updates"
            ):
                print("Update:", event)

        final_state = (await graph.aget_state(config)).values
        print("\n=== FINAL INCIDENT STATE ===")
        print(f"Root Cause: {final_state.get('hypothesis')}")
        print(f"Iterations: {final_state.get('iteration')}")
        print(f"Outcome:    {final_state.get('remediation_result')}")


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
