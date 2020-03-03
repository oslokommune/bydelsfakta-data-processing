import copy
import pytest
import common.event
from common.event import event_handler, PrefixNotInInputError, ExtraPrefixesInInputError

event = {
    "execution_name": "dataset-uuid",
    "task": "bydelsfakta",
    "payload": {
        "pipeline": {
            "id": "instance-id",
            "task_config": {"bydelsfakta": {"type": "status"}},
        },
        "output_dataset": {
            "id": "dataset-out",
            "version": "1",
            "edition": "20200123",
            "s3_prefix": "%stage%/yellow/dataset-out/1/20200123/",
        },
        "step_data": {
            "s3_input_prefixes": {"dataset-in": "raw/green/dataset-in/1/20181115/"},
            "status": "??",
            "errors": [],
        },
    },
}


@pytest.fixture
def read_from_s3(mocker):
    read_from_s3 = mocker.patch.object(common.event, "read_from_s3")
    read_from_s3.return_value = "fake-dataframe"
    return read_from_s3


@pytest.fixture
def find_s3_key(mocker):
    find_s3_key = mocker.patch.object(common.event, "find_s3_key")
    find_s3_key.return_value = "raw/green/dataset-in/1/20181115/file.csv"
    return find_s3_key


def test_event_handler(read_from_s3, find_s3_key):
    @event_handler(df="dataset-in")
    def fn(df, output_prefix, type_of_ds):
        assert df == "fake-dataframe"
        assert (
            output_prefix == "intermediate/yellow/dataset-out/1/20200123/bydelsfakta/"
        )
        assert type_of_ds == "status"

    assert fn(event, None) == {
        "status": "OK",
        "errors": [],
        "s3_input_prefixes": {
            "dataset-out": "intermediate/yellow/dataset-out/1/20200123/bydelsfakta/"
        },
    }

    find_s3_key.assert_called_with("raw/green/dataset-in/1/20181115/")
    read_from_s3.assert_called_with("raw/green/dataset-in/1/20181115/file.csv")


def test_wrong_key(read_from_s3, find_s3_key):
    @event_handler(df="not-exists", i_exist="dataset-in")
    def fn(df, output_prefix, type_of_ds):
        pass

    with pytest.raises(PrefixNotInInputError):
        fn(event, None)


def test_unexpected_input(read_from_s3, find_s3_key):
    @event_handler(df="dataset-in")
    def fn(df, output_prefix, type_of_ds):
        pass

    e = copy.deepcopy(event)
    e["payload"]["step_data"]["s3_input_prefixes"]["extra-input"] = "oh-no"
    with pytest.raises(ExtraPrefixesInInputError):
        fn(e, None)


def test_not_matching_argument_df(read_from_s3, find_s3_key):
    @event_handler(foo="dataset-in")
    def no_match_df(bar, output_prefix, type_of_ds):
        pass

    with pytest.raises(TypeError) as excinfo:
        no_match_df(event, None)
    assert "foo" in str(excinfo.value)


def test_not_matching_argument_output_prefix(read_from_s3, find_s3_key):
    @event_handler(foo="dataset-in")
    def no_match_output_prefix(foo, output_key, type_of_ds):
        pass

    with pytest.raises(TypeError) as excinfo:
        no_match_output_prefix(event, None)
    assert "output_prefix" in str(excinfo.value)


def test_not_matching_argument_type_of_ds(read_from_s3, find_s3_key):
    @event_handler(foo="dataset-in")
    def no_match_type(foo, output_prefix, hype):
        pass

    with pytest.raises(TypeError) as excinfo:
        no_match_type(event, None)
    assert "type_of_ds" in str(excinfo.value)


def test_argument_order(read_from_s3, find_s3_key):
    @event_handler(foo="dataset-in")
    def fn(type_of_ds, foo, output_prefix):
        assert foo == "fake-dataframe"
        assert (
            output_prefix == "intermediate/yellow/dataset-out/1/20200123/bydelsfakta/"
        )
        assert type_of_ds == "status"

    fn(event, None)


def test_proxy(read_from_s3, find_s3_key):
    def _fn(df, output_prefix, type_of_ds):
        assert df == "fake-dataframe"
        assert (
            output_prefix == "intermediate/yellow/dataset-out/1/20200123/bydelsfakta/"
        )
        assert type_of_ds == "status"

    @event_handler(df="dataset-in")
    def fn(*args, **kwargs):
        _fn(*args, **kwargs)

    fn(event, None)
