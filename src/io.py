from pathlib import Path
import pandas as pd, yaml, joblib


def load_raw(base_dir: Path = Path("data_raw")) -> dict[str, pd.DataFrame]:
    dfs: dict[str, pd.DataFrame] = {}
    for path in base_dir.rglob("*"):
        if path.suffix.lower() not in {".csv", ".xlsx", ".yaml"}:
            continue
        if path.suffix.lower() == ".csv":
            df = pd.read_csv(path)
        elif path.suffix.lower() == ".xlsx":
            df = pd.read_excel(path, sheet_name=0)
        else:
            with path.open() as fh:
                data = yaml.safe_load(fh)
            df = pd.json_normalize(data)
        df.columns = [
            col.strip().lower().replace(" ", "_") for col in df.columns
        ]
        dfs[path.stem] = df
    return dfs


if __name__ == "__main__":
    dfs = load_raw()
    summary = [
        (k, len(v), len(v.columns))
        if isinstance(v, pd.DataFrame)
        else (k, "yaml", "yaml")
        for k, v in dfs.items()
    ]
    print(pd.DataFrame(summary, columns=["file", "rows", "cols"]))
    Path("data_interim").mkdir(exist_ok=True)
    joblib.dump(dfs, "data_interim/raw_dfs.pkl")
