import pandas.testing as pdt


def assert_unordered_equality(df1, df2, **kwargs):
    df1 = df1.sort_index(axis=1)
    df1 = df1.sort_values(by=list(df1.columns)).reset_index(drop=True)
    df2 = df2.sort_index(axis=1)
    df2 = df2.sort_values(by=list(df2.columns)).reset_index(drop=True)
    pdt.assert_frame_equal(df1, df2, **kwargs)
