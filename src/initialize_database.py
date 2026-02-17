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


def main():
    """Initialize database and load existing data."""
    
    # Define paths
    project_dir = Path(__file__).parent.parent
    data_dir = project_dir / "data"
    processed_dir = data_dir / "processed"
    db_path = data_dir / "bird_ringing.db"
    
    # CSV file to load
    csv_file = processed_dir / "processed_nidingen_data.csv"
    
    print("=" * 60)
    print("Bird Ringing Database Initialization")
    print("=" * 60)
    
    # Step 1: Initialize database
    print("\nStep 1: Creating database and schema...")
    with BirdRingingDB(db_path, read_only=False) as db:
        db.initialize_schema()
        
        # Step 2: Load CSV data
        if csv_file.exists():
            print(f"\nStep 2: Loading data from {csv_file.name}...")
            db.load_csv_to_table(
                csv_path=csv_file,
                table_name="ring_records",
                if_exists="replace"  # Use 'append' for incremental loads
            )
        else:
            print(f"\nWarning: CSV file not found at {csv_file}")
            print("Skipping data load. Run preprocessing first.")
        
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
        
        # Step 4: Optimize database
        print("\nStep 4: Optimizing database...")
        db.optimize_database()
    
    print("\n" + "=" * 60)
    print("Database initialization complete!")
    print(f"Database location: {db_path}")
    print("=" * 60)
    
    # Step 5: Demonstrate Polars processing
    print("\nStep 5: Example Polars data processing...")
    
    if csv_file.exists():
        # Load with Polars
        processor = BirdDataProcessor()
        df = processor.load_csv(csv_file)
        
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
