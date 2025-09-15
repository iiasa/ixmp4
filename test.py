import pandas as pd

import ixmp4

an = pd.read_csv("tests/fixtures/small/annual.csv")

mp = ixmp4.Platform("dev-private")
run = mp.runs.get("versioning", "test", 1)
run.set_as_default()

with run.transact("remove annual data again"):
    run.iamc.remove(an, type=ixmp4.DataPoint.Type.ANNUAL)

print(run.iamc.tabulate())
