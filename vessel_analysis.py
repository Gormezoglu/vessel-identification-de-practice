import pandas as pd

def is_valid_imo(imo):
    """
    Validates an IMO number.

    An IMO number consists of the three letters "IMO" followed by seven digits. 
    The first six of these are a sequential unique number, while the seventh is a check digit.
    The integrity of an IMO number can be verified using its check digit. 
    This is done by multiplying each of the first six digits by a factor of 7 to 2 respectively, 
    corresponding to their position from left to right. The rightmost digit of this sum is the check digit.

    For example, for IMO 9074729:
    (9×7) + (0×6) + (7×5) + (4×4) + (7×3) + (2×2) = 63 + 0 + 35 + 16 + 21 + 4 = 139
    The rightmost digit of this sum, 9, is the check digit. [Source: https://tarkistusmerkit.teppovuori.fi/coden.htm]
    """
    if not isinstance(imo, int) or len(str(imo)) != 7:
        return False
    imo_str = str(imo)
    check_digit = int(imo_str[-1])
    calculated_check_digit = sum(int(digit) * (7 - i) for i, digit in enumerate(imo_str[:-1])) % 10
    return check_digit == calculated_check_digit

def analyze_data(file_path):
    """
    Performs data cleaning and analysis on the vessel dataset.
    """
    df = pd.read_csv(file_path)

    df.info()

    # Drop columns with more than 90% missing values
    df_cleaned = df.dropna(thresh=len(df) * 0.1, axis=1)

    # Filter out rows with invalid IMO numbers
    df_cleaned = df_cleaned[df_cleaned["imo"].apply(is_valid_imo)].copy()

    df_cleaned.info()

    duplicate_imos = df_cleaned[df_cleaned.duplicated("imo", keep=False)]["imo"].value_counts()
    print(duplicate_imos)

    # # --- Analyze a sample group ---
    sample_imo = 9710749
    sample_group = df_cleaned[df_cleaned['imo'] == sample_imo]
    # # print(sample_group)

    # --- Golden Record Creation ---
    def create_golden_record(group):
        # Sort by UpdateDate to get the most recent record first
        group = group.sort_values(by='UpdateDate', ascending=False)
        
        golden_record = {}
        
        # Handle different attribute types
        for col in group.columns:
            if col in ['imo']:
                golden_record[col] = group[col].iloc[0]
            elif col in ['mmsi', 'callsign', 'name']:
                golden_record[col] = list(group[col].unique())
            elif col.startswith('last_position') or col in ['destination', 'draught.1', 'eta']:
                golden_record[col] = group[col].iloc[0]
            else:
                # For other static data, take the most frequent value (mode)
                golden_record[col] = group[col].mode().iloc[0] if not group[col].mode().empty else None

        return pd.Series(golden_record)

    golden_records = df_cleaned.groupby('imo').apply(create_golden_record)

    print(golden_records.head())

    print(golden_records.loc[sample_imo])

    # Save the golden records to a CSV file
    golden_records.to_csv("golden_records.csv", index=False)

    print("Golden records created and saved to golden_records.csv")

if __name__ == "__main__":
    file_path = "case_study_dataset_202509152039.csv"
    analyze_data(file_path)