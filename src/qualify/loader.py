from sqlalchemy.engine import Engine

class DatabaseLoader:
    """
    Class responsible for loading the processed DataFrames into the database.
    """
    def __init__(self, engine: Engine):
        """
        Initializes the loader with the database engine.
        """
        self.engine = engine

    def load_data(self, dim_drivers, dim_constructors, dim_circuits, dim_races, dim_status, fact_qualifying, fact_pit_stops, fact_race_results):
        """
        Executes the complete loading pipeline.
        
        Args:
            dim_drivers (pd.DataFrame): DataFrame of drivers.
            dim_constructors (pd.DataFrame): DataFrame of constructors.
            dim_circuits (pd.DataFrame): DataFrame of circuits.
            dim_races (pd.DataFrame): DataFrame of races.
            dim_status (pd.DataFrame): DataFrame of status.
            fact_qualifying (pd.DataFrame): DataFrame of qualifying.
            fact_pit_stops (pd.DataFrame): DataFrame of pit stops.
            fact_race_results (pd.DataFrame): DataFrame of race results.
        """
        
        from models import create_all_tables
        create_all_tables(self.engine)
        
        try:
            print("Loading dimension tables...")
            dim_drivers.to_sql('dim_drivers', con=self.engine, if_exists='append', index=False)
            dim_constructors.to_sql('dim_constructors', con=self.engine, if_exists='append', index=False)
            dim_circuits.to_sql('dim_circuits', con=self.engine, if_exists='append', index=False)
            dim_races.to_sql('dim_races', con=self.engine, if_exists='append', index=False)
            dim_status.to_sql('dim_status', con=self.engine, if_exists='append', index=False)
            print("Dimension tables loaded successfully.")

            print("Loading fact tables...")
            fact_qualifying.to_sql('fact_qualifying', con=self.engine, if_exists='append', index=False, chunksize=10000)
            fact_pit_stops.to_sql('fact_pit_stops', con=self.engine, if_exists='append', index=False, chunksize=10000)
            fact_race_results.to_sql('fact_race_results', con=self.engine, if_exists='append', index=False, chunksize=10000)
            print("Fact tables loaded successfully.")
            
        except Exception as e:
            print(f"An error occurred during data loading: {e}")
            raise
            
        print("\nData Loading Successfully Completed ")