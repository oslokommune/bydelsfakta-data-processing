district_01_time_series_list = [
    {
        "id": "00",
        "geography": "NAME",
        "values": [{"value": "d1_00_2018", "ratio": "d1_00_2018_ratio", "date": 2018}],
        "avgRow": False,
        "totalRow": True,
    },
    {
        "id": "01",
        "geography": "Bydel Gamle Oslo",
        "values": [{"value": "d1_01_2018", "ratio": "d1_01_2018_ratio", "date": 2018}],
        "avgRow": True,
        "totalRow": False,
    },
    {
        "id": "0101",
        "geography": "Lodalen",
        "values": [
            {"value": "d1_0101_2018", "ratio": "d1_0101_2018_ratio", "date": 2018}
        ],
        "avgRow": False,
        "totalRow": False,
    },
    {
        "id": "0102",
        "geography": "Grønland",
        "values": [
            {"value": "d1_0102_2018", "ratio": "d1_0102_2018_ratio", "date": 2018}
        ],
        "avgRow": False,
        "totalRow": False,
    },
]

output_list = [
    {
        "bydel_id": "01",
        "template": "c",
        "data": [
            {
                "id": "00",
                "geography": "Oslo i alt",
                "values": [
                    [
                        {
                            "value": "d1_00_2017",
                            "ratio": "d1_00_2017_ratio",
                            "date": 2017,
                        },
                        {
                            "value": "d1_00_2018",
                            "ratio": "d1_00_2018_ratio",
                            "date": 2018,
                        },
                    ],
                    [
                        {"value": "d2_00_2017", "date": 2017},
                        {"value": "d2_00_2018", "date": 2018},
                    ],
                ],
                "avgRow": False,
                "totalRow": True,
            },
            {
                "id": "01",
                "geography": "Bydel Gamle Oslo",
                "values": [
                    [
                        {
                            "value": "d1_01_2017",
                            "ratio": "d1_01_2017_ratio",
                            "date": 2017,
                        },
                        {
                            "value": "d1_01_2018",
                            "ratio": "d1_01_2018_ratio",
                            "date": 2018,
                        },
                    ],
                    [
                        {"value": "d2_01_2017", "date": 2017},
                        {"value": "d2_01_2018", "date": 2018},
                    ],
                ],
                "avgRow": True,
                "totalRow": False,
            },
            {
                "id": "0101",
                "geography": "Lodalen",
                "values": [
                    [
                        {
                            "value": "d1_0101_2017",
                            "ratio": "d1_0101_2017_ratio",
                            "date": 2017,
                        },
                        {
                            "value": "d1_0101_2018",
                            "ratio": "d1_0101_2018_ratio",
                            "date": 2018,
                        },
                    ],
                    [
                        {"value": "d2_0101_2017", "date": 2017},
                        {"value": "d2_0101_2018", "date": 2018},
                    ],
                ],
                "avgRow": False,
                "totalRow": False,
            },
            {
                "id": "0102",
                "geography": "Grønland",
                "values": [
                    [
                        {
                            "value": "d1_0102_2017",
                            "ratio": "d1_0102_2017_ratio",
                            "date": 2017,
                        },
                        {
                            "value": "d1_0102_2018",
                            "ratio": "d1_0102_2018_ratio",
                            "date": 2018,
                        },
                    ],
                    [
                        {"value": "d2_0102_2017", "date": 2017},
                        {"value": "d2_0102_2018", "date": 2018},
                    ],
                ],
                "avgRow": False,
                "totalRow": False,
            },
        ],
    },
    {
        "bydel_id": "02",
        "template": "c",
        "data": [
            {
                "id": "00",
                "geography": "Oslo i alt",
                "values": [
                    [
                        {
                            "value": "d1_00_2017",
                            "ratio": "d1_00_2017_ratio",
                            "date": 2017,
                        },
                        {
                            "value": "d1_00_2018",
                            "ratio": "d1_00_2018_ratio",
                            "date": 2018,
                        },
                    ],
                    [
                        {"value": "d2_00_2017", "date": 2017},
                        {"value": "d2_00_2018", "date": 2018},
                    ],
                ],
                "avgRow": False,
                "totalRow": True,
            },
            {
                "id": "02",
                "geography": "Bydel Grünerløkka",
                "values": [
                    [
                        {
                            "value": "d1_02_2017",
                            "ratio": "d1_02_2017_ratio",
                            "date": 2017,
                        },
                        {
                            "value": "d1_02_2018",
                            "ratio": "d1_02_2018_ratio",
                            "date": 2018,
                        },
                    ],
                    [
                        {"value": "d2_02_2017", "date": 2017},
                        {"value": "d2_02_2018", "date": 2018},
                    ],
                ],
                "avgRow": True,
                "totalRow": False,
            },
            {
                "id": "0201",
                "geography": "Grünerløkka vest",
                "values": [
                    [
                        {
                            "value": "d1_0201_2017",
                            "ratio": "d1_0201_2017_ratio",
                            "date": 2017,
                        },
                        {
                            "value": "d1_0201_2018",
                            "ratio": "d1_0201_2018_ratio",
                            "date": 2018,
                        },
                    ],
                    [
                        {"value": "d2_0201_2017", "date": 2017},
                        {"value": "d2_0201_2018", "date": 2018},
                    ],
                ],
                "avgRow": False,
                "totalRow": False,
            },
            {
                "id": "0202",
                "geography": "Grünerløkka øst",
                "values": [
                    [
                        {
                            "value": "d1_0202_2017",
                            "ratio": "d1_0202_2017_ratio",
                            "date": 2017,
                        },
                        {
                            "value": "d1_0202_2018",
                            "ratio": "d1_0202_2018_ratio",
                            "date": 2018,
                        },
                    ],
                    [
                        {"value": "d2_0202_2017", "date": 2017},
                        {"value": "d2_0202_2018", "date": 2018},
                    ],
                ],
                "avgRow": False,
                "totalRow": False,
            },
        ],
    },
    {
        "bydel_id": "00",
        "template": "c",
        "data": [
            {
                "id": "00",
                "geography": "Oslo i alt",
                "values": [
                    [
                        {
                            "value": "d1_00_2017",
                            "ratio": "d1_00_2017_ratio",
                            "date": 2017,
                        },
                        {
                            "value": "d1_00_2018",
                            "ratio": "d1_00_2018_ratio",
                            "date": 2018,
                        },
                    ],
                    [
                        {"value": "d2_00_2017", "date": 2017},
                        {"value": "d2_00_2018", "date": 2018},
                    ],
                ],
                "avgRow": False,
                "totalRow": True,
            },
            {
                "id": "01",
                "geography": "Bydel Gamle Oslo",
                "values": [
                    [
                        {
                            "value": "d1_01_2017",
                            "ratio": "d1_01_2017_ratio",
                            "date": 2017,
                        },
                        {
                            "value": "d1_01_2018",
                            "ratio": "d1_01_2018_ratio",
                            "date": 2018,
                        },
                    ],
                    [
                        {"value": "d2_01_2017", "date": 2017},
                        {"value": "d2_01_2018", "date": 2018},
                    ],
                ],
                "avgRow": False,
                "totalRow": False,
            },
            {
                "id": "02",
                "geography": "Bydel Grünerløkka",
                "values": [
                    [
                        {
                            "value": "d1_02_2017",
                            "ratio": "d1_02_2017_ratio",
                            "date": 2017,
                        },
                        {
                            "value": "d1_02_2018",
                            "ratio": "d1_02_2018_ratio",
                            "date": 2018,
                        },
                    ],
                    [
                        {"value": "d2_02_2017", "date": 2017},
                        {"value": "d2_02_2018", "date": 2018},
                    ],
                ],
                "avgRow": False,
                "totalRow": False,
            },
        ],
    },
]
