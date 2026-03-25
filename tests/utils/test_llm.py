import litellm

from dlme_airflow.utils.llm import complete, DEFAULT_MODEL


def test_default_model():
    assert DEFAULT_MODEL == "claude-sonnet-4-20250514"


def test_complete_calls_litellm(mocker):
    mock_response = mocker.MagicMock()
    mock_response.choices[0].message.content = "Hello!"
    mocker.patch("dlme_airflow.utils.llm.litellm.completion", return_value=mock_response)

    result = complete([{"role": "user", "content": "Hi"}])

    assert result == "Hello!"
    litellm.completion.assert_called_once_with(
        model=DEFAULT_MODEL,
        messages=[{"role": "user", "content": "Hi"}],
    )


def test_complete_uses_custom_model(mocker):
    mock_response = mocker.MagicMock()
    mock_response.choices[0].message.content = "Hi!"
    mocker.patch("dlme_airflow.utils.llm.litellm.completion", return_value=mock_response)

    complete([{"role": "user", "content": "Hi"}], model="gpt-4o")

    litellm.completion.assert_called_once_with(
        model="gpt-4o",
        messages=[{"role": "user", "content": "Hi"}],
    )


def test_complete_passes_extra_kwargs(mocker):
    mock_response = mocker.MagicMock()
    mock_response.choices[0].message.content = "Response"
    mocker.patch("dlme_airflow.utils.llm.litellm.completion", return_value=mock_response)

    complete([{"role": "user", "content": "Hi"}], temperature=0.5, max_tokens=100)

    litellm.completion.assert_called_once_with(
        model=DEFAULT_MODEL,
        messages=[{"role": "user", "content": "Hi"}],
        temperature=0.5,
        max_tokens=100,
    )
