import litellm

DEFAULT_MODEL = "claude-sonnet-4-20250514"


def complete(messages: list[dict], model: str = DEFAULT_MODEL, **kwargs) -> str:
    """Send a chat completion request via LiteLLM.

    Args:
        messages: List of message dicts with 'role' and 'content' keys.
        model: LiteLLM model string. Defaults to claude-sonnet-4-20250514.
        **kwargs: Additional arguments passed to litellm.completion().

    Returns:
        The text content of the first choice response.
    """
    response = litellm.completion(model=model, messages=messages, **kwargs)
    return response.choices[0].message.content
