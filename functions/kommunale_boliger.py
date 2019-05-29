from pprint import pprint
from common.aws import read_from_s3, write_to_intermediate
from common.transform import status, historic
from common.aggregateV2 import Aggregate
from common.output import Output, Metadata
from common.templates import TemplateA, TemplateB


METADATA = {
    "status": Metadata(heading="Kommunale boliger av boligmassen i alt", series=[]),
    "historic": Metadata(heading="Kommunale boliger av boligmassen i alt", series=[]),
}


def housing(key):
    df = read_from_s3(key)
    df["total"] = (
        df["blokk_leiegaard_el"]
        + df["forretningsgaard_bygg_for_felleshusholdning_el"]
        + df["frittliggende_enebolig_eller_vaaningshus"]
        + df[
            "horisontaldelt_tomannsbolig_eller_annet_boligbygg_med_mindre_enn_3_etasjer"
        ]
        + df["hus_i_kjede_rekkehus_terrasse_hus_vertikaldelt_tomannsbolig"]
    )
    return df


def generate(df_municipal, df_housing):
    agg = Aggregate({"antall_boliger": "sum", "total": "sum"})

    df = agg.merge(df_municipal, df_housing)
    df = agg.aggregate(df)
    df = agg.add_ratios(df, ["antall_boliger"], ["total"])
    return df


def start(*, dataset_type, municipal_key, housing_key):
    df_municipal = read_from_s3(municipal_key)
    df_housing = housing(housing_key)

    if dataset_type == "status":
        dfs = status(df_municipal, df_housing)
        template = TemplateA()
    elif dataset_type == "historic":
        dfs = historic(df_municipal, df_housing)
        template = TemplateB()

    df = generate(*dfs)
    return Output(
        values=["antall_boliger"],
        df=df,
        template=template,
        metadata=METADATA[dataset_type],
    ).generate_output()


def handle(event, context):
    dataset_type = event["config"]["type"]
    jsonl = start(
        dataset_type=dataset_type,
        municipal_key=event["input"]["kommunale-boliger"],
        housing_key=event["input"]["boligmengde-etter-boligtype"],
    )
    write_to_intermediate(output_key=event["output"], output_list=jsonl)


if __name__ == "__main__":
    data = start(
        dataset_type="status",
        municipal_key="raw/green/kommunale-boliger/version=1/edition=20190523T211529/Kommunale_boliger(1.1.2017-1.1.2019-v01).csv",
        housing_key="raw/green/boligmengde-etter-boligtype/version=1/edition=20190524T105717/Boligmengde_etter_boligtype(2011-2017-v01).csv",
    )
    pprint(data)
