"""
Purpose for this function is utility for datasets that needs to be enriched
with the total populations in districts and subdistricts

input dataframe must have columns: date', 'delbydel_id', 'delbydel_navn', 'bydel_id', 'bydel_navn', 'kjønn' and '0' to 'n'
example: | date | delbydel_id | delbydel_navn | bydel_id | bydel_navn | kjønn | 0 | 1 | 2 | ..... | n |

returns dataframe with columns: | date | delbydel_id | delbydel_navn | bydel_id | bydel_navn | population
"""


def generate_population_df(population_raw, min_age=0, max_age=200):

    if max_age > int(population_raw.columns[-1]):
        max_age = int(population_raw.columns[-1])

    df = population_raw.loc[:, "date":"kjonn"]
    df["population"] = population_raw.loc[:, f"{min_age}":f"{max_age}"].sum(axis=1)

    df = (
        df.groupby(["date", "delbydel_id", "delbydel_navn", "bydel_id", "bydel_navn"])
        .sum()
        .reset_index()
    )
    return df[
        ["date", "delbydel_id", "delbydel_navn", "bydel_id", "bydel_navn", "population"]
    ]
