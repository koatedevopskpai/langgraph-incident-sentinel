import os
from functools import lru_cache
from urllib.parse import quote

from dotenv import load_dotenv
from langchain_openai import ChatOpenAI

load_dotenv()


def get_database_url() -> str:
    """
    Resolve the checkpointer DSN.

    ``DATABASE_URL`` wins when present. Otherwise the DSN is assembled from the
    ``POSTGRES_*`` variables so the password only ever lives in an untracked ``.env``
    file (shared with docker-compose) rather than in committed YAML.
    """
    explicit = os.getenv("DATABASE_URL")
    if explicit:
        return explicit

    password = os.getenv("POSTGRES_PASSWORD")
    if not password:
        raise RuntimeError(
            "POSTGRES_PASSWORD is not set. Copy .env.example to .env and choose a "
            "password, or set DATABASE_URL explicitly."
        )

    user = os.getenv("POSTGRES_USER", "sentinel")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5433")
    database = os.getenv("POSTGRES_DB", "langgraph_checkpoints")
    return (
        f"postgresql://{quote(user, safe='')}:{quote(password, safe='')}"
        f"@{host}:{port}/{database}"
    )


@lru_cache(maxsize=1)
def get_llm() -> ChatOpenAI:
    """
    Lazily construct the chat model.

    Deferring construction keeps module imports free of credential side effects, so the
    graph can be imported (and mocked) offline without OPENAI_API_KEY being present.
    """
    return ChatOpenAI(model="gpt-4o-mini", temperature=0)
