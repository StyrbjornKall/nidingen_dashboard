"""
Example script demonstrating how to initialize the database and load data.

This script shows the basic workflow for setting up the bird ringing database
and loading existing CSV data into DuckDB.
"""

from pathlib import Path
import sys

# Add src to path
src_path = Path(__file__).parent
sys.path.insert(0, str(src_path))

from db_manager import BirdRingingDB
from data_processor import BirdDataProcessor

from dotenv import load_dotenv
import os
load_dotenv()

PROJECT_DIR = os.getenv('PROJECT_DIR')
PROCESSED_DATA_DIR = os.getenv('PROCESSED_DATA_DIR')
EBIRD_DIR = os.getenv('METADATA_DIR')
ARTFAKTA_DIR = os.getenv('ARTFAKTA_DIR')


def main():
    """Initialize database and load existing data."""
    
    # Define paths
    project_dir = Path(PROJECT_DIR)
    data_dir = project_dir / "data"
    processed_dir = Path(PROCESSED_DATA_DIR)
    ebird_dir = Path(EBIRD_DIR)
    artfakta_dir = Path(ARTFAKTA_DIR)
    db_path = data_dir / "bird_ringing.db"
    
    # CSV file to load
    preprocessed_data_path = processed_dir / "processed_nidingen_data.csv"
    preprocessed_metadata_path = processed_dir / "combined_species_metadata.csv"
    
    print("=" * 60)
    print("Bird Ringing Database Initialization")
    print("=" * 60)
    
    # Step 1: Initialize database
    print("\nStep 1: Creating database and schema...")
    with BirdRingingDB(db_path, read_only=False) as db:
        db.initialize_schema()
        
        # Step 2: Load ringing records
        if preprocessed_data_path.exists():
            print(f"\nStep 2a: Loading ringing data from {preprocessed_data_path.name}...")
            db.load_csv_to_table(
                csv_path=preprocessed_data_path,
                table_name="ring_records",
                if_exists="replace"
            )
        else:
            print(f"\nWarning: CSV file not found at {preprocessed_data_path}")
            print("Skipping ringing data load. Run preprocessing first.")
        
        # Step 2b: Load species metadata
        if preprocessed_metadata_path.exists():
            print(f"\nStep 2b: Loading species metadata from {preprocessed_metadata_path.name}...")
            import polars as ppl
            meta_df = ppl.read_csv(preprocessed_metadata_path, infer_schema_length=10000)
            
            # Keep only rows that have a swedish_name (these are the Artfakta entries
            # with taxonomy info that can be joined to ring_records)
            meta_df = meta_df.filter(
                ppl.col("swedish_name").is_not_null() & (ppl.col("swedish_name") != "")
            )
            
            # Drop columns not in the species_metadata table schema
            keep_cols = [
                "swedish_name", "species_code", "scientific_name", "english_name",
                "taxon_id", "taxon_order", "category", "order_scientific_name",
                "family_english_name", "family_scientific_name", "family_code",
                "auktor", "taxonkategori", "extinct", "extinct_year",
                "com_name_codes", "sci_name_codes", "banding_codes", "report_as",
            ]
            meta_df = meta_df.select([c for c in keep_cols if c in meta_df.columns])
            
            # Drop and recreate the table
            db.conn.execute("DROP TABLE IF EXISTS species_metadata")
            db.initialize_species_metadata_schema()
            
            # Insert using DuckDB's native Polars integration
            db.conn.execute("""
                INSERT INTO species_metadata
                SELECT * FROM meta_df
            """)
            row_count = db.conn.execute("SELECT COUNT(*) FROM species_metadata").fetchone()[0]
            print(f"Successfully loaded {row_count} species metadata records.")
        else:
            print(f"\nWarning: Metadata CSV not found at {preprocessed_metadata_path}")
            print("Skipping metadata load.")
        
        # Step 3: Display summary statistics
        print("\nStep 3: Database Summary Statistics")
        print("-" * 60)
        stats = db.get_summary_stats()
        
        print(f"Total records: {stats['total_records']:,}")
        print(f"Date range: {stats['date_range'][0]} to {stats['date_range'][1]}")
        print(f"Unique species: {stats['unique_species']}")
        print(f"Unique ringers: {stats['unique_ringers']}")
        
        print(f"\nTop 10 species by observation count:")
        for species_code, swedish_name, count in stats['top_species']:
            print(f"  {species_code:8} ({swedish_name:25}): {count:6,} records")
        
        # Step 4: Generate TOTAL aggregate species
        print("\nStep 4: Generating TOTAL aggregate species...")
        db.ensure_total_species()

        # Step 5: Optimize database
        print("\nStep 5: Optimizing database...")
        db.optimize_database()
    
    print("\n" + "=" * 60)
    print("Database initialization complete!")
    print(f"Database location: {db_path}")
    print("=" * 60)
    
    # Step 5: Demonstrate Polars processing
    print("\nStep 5: Example Polars data processing...")
    
    if preprocessed_data_path.exists():
        # Load with Polars
        processor = BirdDataProcessor()
        df = processor.load_csv(preprocessed_data_path)
        
        # Add time features
        df = processor.add_time_features(df)
        
        # Get species summary
        summary = processor.get_species_summary(df)
        
        print(f"\nProcessed {len(df):,} records")
        print(f"Found {len(summary)} unique species")
        print("\nTop 5 species by unique individuals:")
        print(summary.head(5).select([
            "species_code", 
            "swedish_name", 
            "unique_individuals",
            "total_records"
        ]))


if __name__ == "__main__":
    main()
