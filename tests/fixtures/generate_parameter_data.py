import random
from pathlib import Path

import pandas as pd

here = Path(__file__).parent

# indexsets = [f"Indexset {i}" for i in range(60)] # for 'big'
indexsets = [f"Indexset {i}" for i in range(1)]  # for 'small'
elements_list = [[i for i in range(1000)] for x in range(len(indexsets))]
pd.DataFrame({"name": indexsets, "elements": elements_list}).to_csv(
    "indexsets.csv", index=False
)

parameter_data = [random.sample(range(1000), len(indexsets)) for x in range(10000)]

pd.DataFrame(parameter_data, columns=indexsets).drop_duplicates().to_csv(
    "parameterdata.csv", index=False
)
