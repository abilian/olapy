import pandas as pd
from pandas.util.testing import assert_frame_equal

from .queries import (
    query1,
    query7,
    query8,
    query9,
    query16,
    query_posgres1,
    query_posgres2,
    query_postgres3,
)

# import pytest
# pytest.importorskip("sqlalchemy")


def test_execution_query12(executor):
    df = executor.execute_mdx(query_posgres1)["result"]
    test_df = (
        pd.DataFrame(
            {
                "country": ["France", "Spain", "Switzerland", "United States"],
                "amount": [4, 3, 248, 768],
            }
        )
        .groupby(["country"])
        .sum()
    )

    assert_frame_equal(df, test_df)


def test_execution_query2(executor):
    df = executor.execute_mdx(query_posgres2)["result"]
    test_df = (
        pd.DataFrame(
            {
                "year": [
                    2010,
                    2010,
                    2010,
                    2010,
                    2010,
                    2010,
                    2010,
                    2010,
                    2010,
                    2010,
                    2010,
                    2010,
                    2010,
                ],
                "quarter": [
                    -1,
                    "Q2 2010",
                    "Q2 2010",
                    "Q2 2010",
                    "Q2 2010",
                    "Q2 2010",
                    "Q2 2010",
                    "Q2 2010",
                    "Q2 2010",
                    "Q2 2010",
                    "Q2 2010",
                    "Q2 2010",
                    "Q2 2010",
                ],
                "month": [
                    -1,
                    -1,
                    "May 2010",
                    "May 2010",
                    "May 2010",
                    "May 2010",
                    "May 2010",
                    "May 2010",
                    "May 2010",
                    "May 2010",
                    "May 2010",
                    "May 2010",
                    "May 2010",
                ],
                "day": [
                    -1,
                    -1,
                    -1,
                    "May 12,2010",
                    "May 13,2010",
                    "May 14,2010",
                    "May 15,2010",
                    "May 16,2010",
                    "May 17,2010",
                    "May 18,2010",
                    "May 19,2010",
                    "May 20,2010",
                    "May 21,2010",
                ],
                "amount": [1023, 1023, 1023, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512],
            }
        )
        .groupby(["year", "quarter", "month", "day"])
        .sum()
    )

    assert_frame_equal(df, test_df)


def test_execution_query10(executor):
    df = executor.execute_mdx(query_postgres3)["result"]
    test_df = (
        pd.DataFrame(
            {
                "year": [2010, 2010, 2010, 2010, 2010, 2010, 2010, 2010],
                "quarter": [
                    "Q2 2010",
                    "Q2 2010",
                    "Q2 2010",
                    "Q2 2010",
                    "Q2 2010",
                    "Q2 2010",
                    "Q2 2010",
                    "Q2 2010",
                ],
                "month": [
                    "May 2010",
                    "May 2010",
                    "May 2010",
                    "May 2010",
                    "May 2010",
                    "May 2010",
                    "May 2010",
                    "May 2010",
                ],
                "day": [
                    "May 19,2010",
                    "May 17,2010",
                    "May 15,2010",
                    "May 13,2010",
                    "May 12,2010",
                    "May 14,2010",
                    "May 16,2010",
                    "May 18,2010",
                ],
                "continent": [
                    "Europe",
                    "Europe",
                    "Europe",
                    "Europe",
                    "Europe",
                    "Europe",
                    "Europe",
                    "Europe",
                ],
                "count": [13, 65, 231, 841, 84, 2, 4, 64],
            }
        )
        .groupby(["year", "quarter", "month", "day", "continent"], sort=False)
        .sum()
    )

    assert_frame_equal(df, test_df)


def test_execution_query1(executor):
    assert executor.execute_mdx(query1)["result"]["amount"][0] == 1023


def test_execution_query7(executor):
    df = executor.execute_mdx(query7)["result"]
    test_df = (
        pd.DataFrame(
            {
                "company": [
                    "Crazy Development",
                    "Crazy Development",
                    "Crazy Development",
                    "Crazy Development",
                    "Crazy Development",
                    "Crazy Development",
                    "Crazy Development",
                    "Crazy Development",
                ],
                "year": [2010, 2010, 2010, 2010, 2010, 2010, 2010, 2010],
                "quarter": [
                    "Q2 2010",
                    "Q2 2010",
                    "Q2 2010",
                    "Q2 2010",
                    "Q2 2010",
                    "Q2 2010",
                    "Q2 2010",
                    "Q2 2010",
                ],
                "month": [
                    "May 2010",
                    "May 2010",
                    "May 2010",
                    "May 2010",
                    "May 2010",
                    "May 2010",
                    "May 2010",
                    "May 2010",
                ],
                "day": [
                    "May 18,2010",
                    "May 16,2010",
                    "May 14,2010",
                    "May 12,2010",
                    "May 13,2010",
                    "May 15,2010",
                    "May 17,2010",
                    "May 19,2010",
                ],
                "continent": [
                    "Europe",
                    "Europe",
                    "Europe",
                    "Europe",
                    "Europe",
                    "Europe",
                    "Europe",
                    "Europe",
                ],
                "amount": [64, 16, 4, 1, 2, 8, 32, 128],
            }
        )
        .groupby(
            ["company", "year", "quarter", "month", "day", "continent"], sort=False
        )
        .sum()
    )

    assert_frame_equal(df, test_df)


def test_execution_query8(executor):
    df = executor.execute_mdx(query8)["result"]
    test_df = (
        pd.DataFrame(
            {
                "continent": ["Europe", "Europe", "Europe"],
                "country": ["Spain", "France", "Switzerland"],
                "amount": [3, 4, 248],
            }
        )
        .groupby(["continent", "country"], sort=False)
        .sum()
    )

    assert_frame_equal(df, test_df)


def test_execution_query9(executor):
    df = executor.execute_mdx(query9)["result"]
    test_df = (
        pd.DataFrame(
            {
                "year": [2010, 2010, 2010, 2010, 2010, 2010, 2010, 2010],
                "quarter": [
                    "Q2 2010",
                    "Q2 2010",
                    "Q2 2010",
                    "Q2 2010",
                    "Q2 2010",
                    "Q2 2010",
                    "Q2 2010",
                    "Q2 2010",
                ],
                "month": [
                    "May 2010",
                    "May 2010",
                    "May 2010",
                    "May 2010",
                    "May 2010",
                    "May 2010",
                    "May 2010",
                    "May 2010",
                ],
                "day": [
                    "May 19,2010",
                    "May 17,2010",
                    "May 15,2010",
                    "May 13,2010",
                    "May 12,2010",
                    "May 14,2010",
                    "May 16,2010",
                    "May 18,2010",
                ],
                "continent": [
                    "Europe",
                    "Europe",
                    "Europe",
                    "Europe",
                    "Europe",
                    "Europe",
                    "Europe",
                    "Europe",
                ],
                "amount": [128, 32, 8, 2, 1, 4, 16, 64],
            }
        )
        .groupby(["year", "quarter", "month", "day", "continent"], sort=False)
        .sum()
    )

    assert_frame_equal(df, test_df)


def test_execution_query16(executor):
    df = executor.execute_mdx(query16)["result"]
    test_df = (
        pd.DataFrame(
            {
                "company": [
                    "Crazy Development",
                    "Crazy Development",
                    "Crazy Development",
                    "Crazy Development",
                    "Crazy Development",
                    "Crazy Development",
                    "Crazy Development",
                    "Crazy Development",
                    "Crazy Development",
                    "Crazy Development",
                    "Crazy Development",
                    "Crazy Development",
                    "Crazy Development",
                    "Crazy Development",
                    "Crazy Development",
                    "Crazy Development",
                    "Crazy Development",
                    "Crazy Development",
                    "Crazy Development",
                    "Crazy Development",
                    "Crazy Development",
                    "Crazy Development",
                    "Crazy Development",
                    "Crazy Development",
                ],
                "article": [
                    -1,
                    -1,
                    -1,
                    -1,
                    -1,
                    -1,
                    -1,
                    -1,
                    -1,
                    "olapy",
                    "olapy",
                    "olapy",
                    "olapy",
                    "olapy",
                    "olapy",
                    "olapy",
                    "olapy",
                    "olapy",
                    "olapy",
                    "olapy",
                    "olapy",
                    "olapy",
                    "olapy",
                    "olapy",
                ],
                "licence": [
                    -1,
                    -1,
                    -1,
                    -1,
                    -1,
                    -1,
                    -1,
                    -1,
                    -1,
                    "Partnership",
                    "Partnership",
                    "Partnership",
                    "Personal",
                    "Personal",
                    "Personal",
                    "Personal",
                    "Personal",
                    "Corporate",
                    "Corporate",
                    "Corporate",
                    "Corporate",
                    "Corporate",
                    "Corporate",
                    "Corporate",
                ],
                "continent": [
                    "America",
                    "Europe",
                    "America",
                    "Europe",
                    "Europe",
                    "America",
                    "Europe",
                    "Europe",
                    "Europe",
                    "Europe",
                    "Europe",
                    "Europe",
                    "Europe",
                    "Europe",
                    "Europe",
                    "Europe",
                    "Europe",
                    "America",
                    "Europe",
                    "America",
                    "Europe",
                    "America",
                    "Europe",
                    "Europe",
                ],
                "country": [
                    -1,
                    -1,
                    "United States",
                    "Switzerland",
                    "France",
                    "United States",
                    "Switzerland",
                    "Switzerland",
                    "France",
                    -1,
                    "Switzerland",
                    "Switzerland",
                    -1,
                    "Switzerland",
                    "France",
                    "Switzerland",
                    "France",
                    -1,
                    -1,
                    "United States",
                    "Switzerland",
                    "United States",
                    "Switzerland",
                    "Switzerland",
                ],
                "city": [
                    -1,
                    -1,
                    -1,
                    -1,
                    -1,
                    "New York",
                    "Geneva",
                    "Lausanne",
                    "Paris",
                    -1,
                    -1,
                    "Lausanne",
                    -1,
                    -1,
                    -1,
                    "Lausanne",
                    "Paris",
                    -1,
                    -1,
                    -1,
                    -1,
                    "New York",
                    "Geneva",
                    "Lausanne",
                ],
                "amount": [
                    768,
                    255,
                    768,
                    248,
                    4,
                    768,
                    128,
                    56,
                    4,
                    96,
                    96,
                    32,
                    15,
                    8,
                    4,
                    8,
                    4,
                    768,
                    144,
                    768,
                    144,
                    768,
                    128,
                    16,
                ],
            }
        )
        .groupby(
            ["company", "article", "licence", "continent", "country", "city"],
            sort=False,
        )
        .sum()
    )

    assert_frame_equal(df, test_df)
