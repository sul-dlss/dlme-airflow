from dlme_airflow.tasks.transform_validation import eval_record_count_formula


def test_eval_record_count_formula():
    assert eval_record_count_formula("6", "/2") == 3
    assert eval_record_count_formula("6", "-2") == 4
    assert eval_record_count_formula("6", "+2") == 8
