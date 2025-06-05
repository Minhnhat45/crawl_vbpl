import pandas as pd

data = pd.read_parquet("./ThuVienPhapLuat/items_ThuVienPhapLuat_11.parquet", engine="pyarrow")

print(data.head())