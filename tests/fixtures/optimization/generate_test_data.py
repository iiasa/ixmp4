import random
from pathlib import Path

import pandas as pd

here = Path(__file__).parent

# # Create indexsets (indexset names) in various sizes
# # small
# pd.Series([f"Indexset {i}" for i in range(100)]).to_csv(
#     here / "small/indexsets.csv", header=["name"], index=False
# )

# # medium
# pd.Series([f"Indexset {i}" for i in range(10000)]).to_csv(
#     here / "medium/indexsets.csv", header=["name"], index=False
# )

# # big
# pd.Series([f"Indexset {i}" for i in range(1000000)]).to_csv(
#     here / "big/indexsets.csv", header=["name"], index=False
# )

# # Create indexsetdata in various sizes
# # small
# pd.Series(range(100)).to_csv(
#     here / "small/indexsetdata.csv", header=["data"], index=False
# )

# # medium
# pd.Series(range(10000)).to_csv(
#     here / "medium/indexsetdata.csv", header=["data"], index=False
# )

# # big
# pd.Series(range(1000000)).to_csv(
#     here / "big/indexsetdata.csv", header=["data"], index=False
# )

# # Create parameters (parameter names) in various sizes
# # small
# pd.Series([f"Parameter {i}" for i in range(100)]).to_csv(
#     here / "small/parameters.csv", header=["name"], index=False
# )

# # medium
# pd.Series([f"Parameter {i}" for i in range(10000)]).to_csv(
#     here / "medium/parameters.csv", header=["name"], index=False
# )

# # big
# pd.Series([f"Parameter {i}" for i in range(1000000)]).to_csv(
#     here / "big/parameters.csv", header=["name"], index=False
# )

# Create parameterdata in various sizes
# NOTE always use 15 indexsets for creation, then drop unnecessary later on
indexsets = [f"Indexset {i}" for i in range(15)]
# small
parameter_data = [random.sample(range(100), len(indexsets)) for x in range(100)]
pd.DataFrame(parameter_data, columns=indexsets).drop_duplicates().to_csv(
    here / "small/parameterdata.csv", index=False
)

# medium
parameter_data = [random.sample(range(10000), len(indexsets)) for x in range(10000)]
pd.DataFrame(parameter_data, columns=indexsets).drop_duplicates().to_csv(
    here / "medium/parameterdata.csv", index=False
)

# big
parameter_data = [random.sample(range(1000000), len(indexsets)) for x in range(1000000)]
pd.DataFrame(parameter_data, columns=indexsets).drop_duplicates().to_csv(
    here / "big/parameterdata.csv", index=False
)
