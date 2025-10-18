from et import F1ETQualifyProcessor
from loader import DatabaseLoader 
from database import engine  
import os
def run_etl_pipeline():
    """
    Main function to execute the F1 ETL pipeline step-by-step.
    """
    print("--- F1 Data ETL Pipeline Started ---")
    
    DATA_DIRECTORY = 'data/raw' 

    PROCESSED_DATA_DIRECTORY = 'data/processed'
    

    if not os.path.exists(PROCESSED_DATA_DIRECTORY):
        os.makedirs(PROCESSED_DATA_DIRECTORY)

    try:
        processor = F1ETQualifyProcessor(data_path=DATA_DIRECTORY)
        
        # Process each dimension first. This cleans the data.
        dim_drivers = processor.process_dim_drivers()
        print("\n** Drivers Dimension **")
        print(dim_drivers.head())
        dim_drivers.to_csv(os.path.join(PROCESSED_DATA_DIRECTORY, 'dim_drivers.csv'), index=False)

        dim_constructors = processor.process_dim_constructors()
        print("\n** Constructors Dimension **")
        print(dim_constructors.head())
        dim_constructors.to_csv(os.path.join(PROCESSED_DATA_DIRECTORY, 'dim_constructors.csv'), index=False)
        

        dim_circuits = processor.process_dim_circuits()
        print("\n** Circuits Dimension **")
        print(dim_circuits.head())
        dim_circuits.to_csv(os.path.join(PROCESSED_DATA_DIRECTORY, 'dim_circuits.csv'), index=False)

        dim_races = processor.process_dim_races()
        print("\n** Races Dimension **")
        print(dim_races.head())
        dim_races.to_csv(os.path.join(PROCESSED_DATA_DIRECTORY, 'dim_races.csv'), index=False)

        dim_status = processor.process_dim_status()
        print("\n** Status Dimension **")
        print(dim_status.head())
        dim_status.to_csv(os.path.join(PROCESSED_DATA_DIRECTORY, 'dim_status.csv'), index=False)

        # 3. Process the fact table, passing the clean dimensions for mapping.
        fact_qualifying = processor.process_fact_qualifying(        )
        print("\n** Qualifying Fact Table (note the mapped FKs) **")
        print(fact_qualifying.head())
        fact_qualifying.to_csv(os.path.join(PROCESSED_DATA_DIRECTORY, 'fact_qualifying.csv'), index=False)

                # 3. Process the fact table, passing the clean dimensions for mapping.
        fact_pit_stops = processor.process_fact_pit_stops()
        print("\n** Pit Stops Fact Table (note the mapped FKs) **")
        print(fact_pit_stops.head())
        fact_pit_stops.to_csv(os.path.join(PROCESSED_DATA_DIRECTORY, 'fact_pit_stops.csv'), index=False)


        fact_race_results = processor.process_fact_race_results() # <-- NUEVA TABLA DE HECHOS
        fact_race_results.to_csv(os.path.join(PROCESSED_DATA_DIRECTORY, 'fact_race_results.csv'), index=False) # <-- GUARDAR NUEVA TABLA DE HECHOS

        # --- VERIFICACIÓN ---
        print("\n** Race Results Fact Table **")
        print(fact_race_results.head())
        db_loader = DatabaseLoader(engine=engine)
        
         #2. Llama al método para cargar todos los DataFrames
        db_loader.load_data(
            dim_drivers=dim_drivers,
            dim_constructors=dim_constructors,
            dim_circuits=dim_circuits,
            dim_races=dim_races,
            dim_status=dim_status,
            fact_qualifying=fact_qualifying,
            fact_pit_stops=fact_pit_stops,
            fact_race_results=fact_race_results
        )
        
        
    except Exception as e:
        print(f"An error occurred during the ETL process: {e}")
        
    print("\n--- F1 Data ETL Pipeline Finished ---")

if __name__ == "__main__":
    run_etl_pipeline()