from harvester.copydir import copydir


def test_copydir(monkeypatch, tmp_path):
    """Mocks standard library's shutil.copytree and time.time to test
    copydir functionality

    @param monkeypatch -- pytest mocking object
    @param tmp_path -- pytest fixture
    """

    def copytree(src, dst):
        dst = tmp_path / "1628113477.966395"
        dst.touch()
        return dst

    def time():
        return 1628113477.966395

    monkeypatch.setattr("time.time", time)

    monkeypatch.setattr("shutil.copytree", copytree)

    copydir(provider="aims")
    assert (tmp_path / "1628113477.966395").exists()
