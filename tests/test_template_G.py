from common.templates import TemplateG
from tests.template_helper import test_df

template = TemplateG(history_columns=["d2"])


def test_df_to_template_g():
    input_df = test_df[
        (test_df.delbydel_id == "0202") & (test_df.date == test_df.date.max())
    ]
    value = template.values(input_df, ["d1"])
    assert value == ["d1_0202_2018", ["d2_0202_2018"]]
