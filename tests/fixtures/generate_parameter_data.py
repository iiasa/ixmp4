import random
from pathlib import Path

import pandas as pd

here = Path(__file__).parent

indexsets = [f"Indexset {i}" for i in range(12)]  # for Volker
elements_list = [[i for i in range(50)] for x in range(len(indexsets))]  # for Volker

# indexsets = [f"Indexset {i}" for i in range(60)] # for 'big'
# indexsets = [f"Indexset {i}" for i in range(1)]  # for 'small'
# # For 'big' or 'small':
# elements_list = [[i for i in range(1000) for x in range(len(indexsets))]]
pd.DataFrame({"name": indexsets, "elements": elements_list}).to_csv(
    "indexsets.csv", index=False
)

parameter_data = [random.sample(range(50), len(indexsets)) for x in range(1000000)]

pd.DataFrame(parameter_data, columns=indexsets).drop_duplicates().to_csv(
    "parameterdata.csv", index=False
)
