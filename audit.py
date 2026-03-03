import os
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(BASE_DIR, "shelter.csv")


def load_data(path: str) -> pd.DataFrame:
    print("Trying to open:", path)
    return pd.read_csv(path, encoding="utf-8-sig")


def basic_profile(df: pd.DataFrame) -> None:
    print("\n===== df.info() =====")
    df.info()

    print("\n===== Missing Values by Column =====")
    missing = df.isna().sum()
    for col, count in missing.items():
        print(f"{col}: {count}")


def fix_missing_fields(df: pd.DataFrame):
    """Add missing flags + extract records needing manual review (no blind imputation)."""
    df = df.copy()

    # ----- Completeness flags -----
    df["缺失村里旗標"] = df["村里"].isna().astype(int)
    df["缺失避難收容處所地址旗標"] = df["避難收容處所地址"].isna().astype(int)
    df["缺失適用災害類別旗標"] = df["適用災害類別"].isna().astype(int)

    # ----- Spatial validity: Taiwan bounding box check -----
    # Note: bounding box only checks numeric plausibility, not "on-land" correctness.
    df["座標超出範圍旗標"] = (
        (df["經度"] < 118) |
        (df["經度"] > 123.6) |
        (df["緯度"] < 21.5) |
        (df["緯度"] > 25.5)
    ).astype(int)

    # ----- Extract rows for manual review -----
    cols_for_review = [
        c for c in ["序號", "避難收容處所名稱", "縣市及鄉鎮市區", "村里", "避難收容處所地址", "經度", "緯度"]
        if c in df.columns
    ]
    missing_address_df = df[df["缺失避難收容處所地址旗標"] == 1][cols_for_review].copy()
    invalid_coordinates_df = df[df["座標超出範圍旗標"] == 1][cols_for_review].copy()

    # ----- Summary -----
    total = len(df)
    counts = {
        "缺失村里旗標": int(df["缺失村里旗標"].sum()),
        "缺失避難收容處所地址旗標": int(df["缺失避難收容處所地址旗標"].sum()),
        "缺失適用災害類別旗標": int(df["缺失適用災害類別旗標"].sum()),
        "座標超出範圍旗標": int(df["座標超出範圍旗標"].sum()),
    }
    summary = {k: v for k, v in counts.items()}
    summary.update({f"{k}比例": (v / total) * 100 for k, v in counts.items()})

    return df, summary, missing_address_df, invalid_coordinates_df


if __name__ == "__main__":
    df = load_data(file_path)

    basic_profile(df)

    df_fixed, summary, missing_address_df, invalid_coordinates_df = fix_missing_fields(df)

    print("\n===== Fix summary =====")
    for k, v in summary.items():
        print(f"{k}: {v:.2f}%" if "比例" in k else f"{k}: {v}")

    print("\n===== Missing address rows (for manual review) =====")
    print(missing_address_df)

    print("\n===== Invalid coordinate rows (out of Taiwan bounding box) =====")
    print(invalid_coordinates_df)

    # exports for GitHub deliverables
    missing_address_df.to_csv(
        os.path.join(BASE_DIR, "missing_address_rows.csv"),
        index=False,
        encoding="utf-8-sig"
    )
    invalid_coordinates_df.to_csv(
        os.path.join(BASE_DIR, "invalid_coordinates_rows.csv"),
        index=False,
        encoding="utf-8-sig"
    )
    df_fixed.to_csv(
        os.path.join(BASE_DIR, "shelter_audited.csv"),
        index=False,
        encoding="utf-8-sig"
    )