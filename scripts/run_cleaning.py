from pathlib import Path
from src.data.clean_data import load_raw_data, handle_missing_values, standardize_columns, remove_duplicates, save_cleaned_data, generate_cleaning_summary
from src.data.validate_data import validate_cleaned_data

def main():
    raw_file = Path("data/raw/enterprise_privileged_access_logs.csv")
    clean_file = Path("data/processed/cleaned_access_logs.csv")
    report_dir = Path("outputs/reports")

    df = load_raw_data(raw_file)
    df_before = df.copy()

    df = handle_missing_values(df)
    df = standardize_columns(df)
    df = remove_duplicates(df)

    validate_cleaned_data(df)

    save_cleaned_data(df, clean_file)

    generate_cleaning_summary(df_before, df, report_dir)

if __name__ == "__main__":
    main()