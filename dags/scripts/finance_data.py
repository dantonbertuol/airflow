import requests
import pandas as pd

datafile = "/tmp/dataset.csv"
api_key = "YOUR_API_KEY"
start_date = "2023-02-14"
end_date = "2023-02-19"
url = 'https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/{}/{}?apiKey={}'.format(start_date, end_date, api_key)
r = requests.get(url)
data = r.json()

if data["count"] > 0:
    # Transforma o dicionário em um dataframe
    df = pd.DataFrame(data["results"])
    df["date"] = start_date

    # Exporta o dataframe para um arquivo CSV
    df.to_csv(datafile, index=False)
