
import os
from pathlib import Path
import pandas as pd
from typing import List
from dotenv import load_dotenv

load_dotenv()
PROJECT_DIR = os.getenv('PROJECT_DIR')
RAW_DATA_DIR = os.getenv('RAW_DATA_DIR')
PROCESSED_DATA_DIR = os.getenv('PROCESSED_DATA_DIR')
METADATA_DIR = os.getenv('METADATA_DIR')

def preprocess_yearly_report(file_path: Path) -> None:
    """Preprocess a yearly report text file from the Nidingen dataset.

    Parameters:
    file_path (Path): The path to the raw data file.
    Returns:
    pd.DataFrame: A cleaned and preprocessed DataFrame.
    """

    # Check if the file exists    
    if not file_path.exists():
        print(f"File {file_path} does not exist.")
        return
    
    save_to = file_path.parent.joinpath(file_path.stem)
    
    # Create output directory for the year if it doesn't exist
    if not save_to.exists():
        os.makedirs(save_to)
    
    # Output files
    meta_files = {
        "Q": save_to.joinpath("meta_header.txt"),
        "M": save_to.joinpath("meta_station.txt"),
        #"H": save_to.joinpath("meta_ringers.txt"), # We don't save this information
        "S": save_to.joinpath("meta_ringer_initials.txt"),
        "L": save_to.joinpath("meta_locations.txt")
    }

    data_file = save_to.joinpath("ring_records.txt")

    # Open output files
    meta_outputs = {k: open(v, "w", encoding="utf-8") for k, v in meta_files.items()}
    data_out = open(data_file, "w", encoding="utf-8")

    with open(file_path, encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            prefix = line[0]
            if prefix in meta_outputs:
                meta_outputs[prefix].write(line)
            elif prefix in ("C", "R"):
                data_out.write(line)

    # Close all files
    for f in meta_outputs.values():
        f.close()
    data_out.close()

    print("Done. Metadata and data files written.")

def preprocess_nidingen_raw_data(file_path: Path) -> pd.DataFrame:
    """
    Preprocess the raw data from the Nidingen dataset.
    
    Parameters:
    file_path (Path): The path to the raw data file.
    
    Returns:
    pd.DataFrame: A cleaned and preprocessed DataFrame.
    """

    df = pd.read_csv(file_path, sep='|', header=None)

    # Rename columns
    rename_dict = {
        0: 'record_type',
        1: 'ring_number',
        2: 'scheme',
        3: 'station_code',
        4: 'station_name',
        5: 'age_code',
        7: 'date',
        8: 'time',
        9: 'species_code',
        10: 'ringer',
        12: 'age',
        17: 'wing_length',
        18: 'weight',
        19: 'fat_score',
        20: 'muscle_score',
        30: 'brood_patch',
        32: 'moult_score',
        34: 'notes'
    }

    df = df.rename(columns=rename_dict)

    # Drop the other columns
    df = df[list(rename_dict.values())]

    # Filter out columns that have >70% NA values
    threshold = 0.7 * len(df)
    df = df.loc[:, df.isna().sum() < threshold]

    # Convert column dtypes
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df['wing_length'] = pd.to_numeric(df['wing_length'], errors='coerce')
    df['weight'] = pd.to_numeric(df['weight'], errors='coerce')
    df['fat_score'] = pd.to_numeric(df['fat_score'], errors='coerce')
    df['muscle_score'] = pd.to_numeric(df['muscle_score'], errors='coerce')
    df['brood_patch'] = pd.to_numeric(df['brood_patch'], errors='coerce')
    df['moult_score'] = pd.to_numeric(df['moult_score'], errors='coerce')

    # Place date and time at the beginning
    df = df[['date', 'time'] + [col for col in df.columns if col not in ['date', 'time']]]

    # Drop duplicate rows
    df = df.drop_duplicates()

    #Drop columns that are only NaN
    df = df.dropna(axis=1, how='all')
    #Drop rows that are only NaN
    df = df.dropna(axis=0, how='all')

    # Filter data
    df = df[df.scheme=='SVS']
    df = df[df.station_name=='0016NID']

    # Drop unnecessary columns
    df = df.drop(columns=['station_code', 'scheme', 'station_name'])
        
    return df


# Get dirs in directory
def get_ringing_data_dirs_in_directory(directory: Path) -> List[Path]:
    """
    Get a list of directories in the specified directory with full paths.
    
    Parameters:
    directory (Path): The path to the directory.
    
    Returns:
    list: A list of full paths to directories.
    """
    return [directory / d for d in os.listdir(directory) if (directory / d).is_dir()]


def get_species_metadata_from_codes(species_codes: List[str], metadata_file: Path) -> pd.DataFrame:
    """
    Get species metadata from a code.
    
    Parameters:
    species_codes (List[str]): The species codes to look up.
    metadata_file (Path): The path to the species metadata file.
    
    Returns:
    pd.DataFrame: A Series containing the species metadata.
    """
    metadata_df = pd.read_csv(metadata_file)
    metadata_df['Sökträff'] = metadata_df.Sökträff.str.upper()  # Ensure Sökträff is lowercase
    metadata_df = metadata_df.rename(columns={'Svenskt namn': 'swedish_name', 'Vetenskapligt namn': 'scientific_name'})  # Rename for merging
    # Ensure codes are lowercase
    queries = pd.DataFrame(data=species_codes, columns=['species_code'])
    
    # Merge the metadata with the queries
    return queries.merge(metadata_df, left_on='species_code', right_on='Sökträff', how='left').drop(columns=['Sökträff','Namnkategori','Auktor','Taxonkategori']).reset_index(drop=True)

def collate_and_preprocess_nidingen_data(file_paths: List[Path]) -> pd.DataFrame:
    """
    Collate and preprocess multiple Nidingen dataset files.
    
    Parameters:
    file_paths (List[Path]): List of file paths to the raw data files.
    
    Returns:
    pd.DataFrame: A single cleaned and preprocessed DataFrame.
    """

    print(f"Collating and preprocessing data from {len(file_paths)} files...")

    dfs = []
    for file_path in file_paths:
        df = preprocess_nidingen_raw_data(file_path)
        dfs.append(df)

    # Concatenate all DataFrames
    combined_df = pd.concat(dfs, ignore_index=True)

    # Drop duplicate rows after concatenation
    combined_df = combined_df.drop_duplicates()

    # Get metadata from species codes
    combined_df = pd.concat([
        combined_df,
        get_species_metadata_from_codes(combined_df.species_code.tolist(), metadata_file=Path(f'{RAW_DATA_DIR}/species_metadata.csv')).drop(
            columns=['species_code']),
        ],
        axis=1
        ).reset_index(drop=True)

    return combined_df

if __name__ == "__main__":
    # verify that the raw data directory exists
    if not os.path.exists(RAW_DATA_DIR):
        print(f"Raw data directory {RAW_DATA_DIR} does not exist. Please check the path and try again.")
        exit(1)
    # verify that the processed data directory exists, if not create it
    if not os.path.exists(PROCESSED_DATA_DIR):
        print(f"Processed data directory {PROCESSED_DATA_DIR} does not exist. Creating it.")
        os.makedirs(PROCESSED_DATA_DIR)
    # verify that the metadata directory exists, if not create it
    if not os.path.exists(METADATA_DIR):
        print(f"Metadata directory {METADATA_DIR} does not exist. Creating it.")
        os.makedirs(METADATA_DIR)

    # Preprocess yearly reports and divide into metadata and data files
    for year in range(2010, 2026, 1):
        preprocess_yearly_report(Path(f'{RAW_DATA_DIR}/0016år-{year}.txt'))

    # Get all directories in the processed data directory
    data_dirs = get_ringing_data_dirs_in_directory(Path(RAW_DATA_DIR))
    data_dirs = [d / "ring_records.txt" for d in data_dirs]

    df = collate_and_preprocess_nidingen_data(data_dirs)

    # Save the combined DataFrame to a new CSV file
    df.to_csv(f'{PROCESSED_DATA_DIR}/processed_nidingen_data.csv', index=False)