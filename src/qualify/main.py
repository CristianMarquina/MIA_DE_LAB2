from et import F1ETQualifyProcessor

def run_etl_pipeline():
    """
    Main function to execute the F1 ETL pipeline step-by-step.
    """
    print("--- F1 Data ETL Pipeline Started ---")
    
    DATA_DIRECTORY = 'data/raw' 
    
    try:
        processor = F1ETQualifyProcessor(data_path=DATA_DIRECTORY)
        
        # Process each dimension first. This cleans the data.
        dim_drivers = processor.process_dim_drivers()
        print("\n** Drivers Dimension **")
        print(dim_drivers.head())
        dim_constructors = processor.process_dim_constructors()
        print("\n** Constructors Dimension **")
        print(dim_constructors.head())
        dim_circuits = processor.process_dim_circuits()
        print("\n** Circuits Dimension **")
        print(dim_circuits.head())
        dim_races = processor.process_dim_races()
        print("\n** Races Dimension **")
        print(dim_races.head())
        # 3. Process the fact table, passing the clean dimensions for mapping.
        fact_qualifying = processor.process_fact_qualifying(
            dim_races=dim_races,
            dim_drivers=dim_drivers,
            dim_constructors=dim_constructors
        )
        print("\n** Qualifying Fact Table (note the mapped FKs) **")
        print(fact_qualifying.head())
        
    except Exception as e:
        print(f"An error occurred during the ETL process: {e}")
        
    print("\n--- F1 Data ETL Pipeline Finished ---")

if __name__ == "__main__":
    run_etl_pipeline()