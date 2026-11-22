import os
from functools import lru_cache

from dotenv import load_dotenv
from langchain_openai import ChatOpenAI

load_dotenv()

DEFAULT_DATABASE_URL = (
    "postgresql://sentinel:sentinel_password@localhost:5433/langgraph_checkpoints"
)


def get_database_url() -> str:
    """Resolve the checkpointer DSN, falling back to the local docker-compose instance."""
    return os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL)


@lru_cache(maxsize=1)
def get_llm() -> ChatOpenAI:
    """
    Lazily construct the chat model.

    Deferring construction keeps module imports free of credential side effects, so the
    graph can be imported (and mocked) offline without OPENAI_API_KEY being present.
    """
    return ChatOpenAI(model="gpt-4o-mini", temperature=0)
