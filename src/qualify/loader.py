# src/qualify/loader.py

from sqlalchemy.engine import Engine

class DatabaseLoader:
    """
    Clase responsable de cargar los DataFrames procesados en la base de datos.
    """
    def __init__(self, engine: Engine):
        """
        Inicializa el cargador con el motor de la base de datos.
        """
        self.engine = engine

    def load_data(self, dim_drivers, dim_constructors, dim_circuits, dim_races, fact_qualifying):
        """
        Ejecuta el pipeline de carga completo.
        
        Args:
            dim_drivers (pd.DataFrame): DataFrame de pilotos.
            dim_constructors (pd.DataFrame): DataFrame de constructores.
            dim_circuits (pd.DataFrame): DataFrame de circuitos.
            dim_races (pd.DataFrame): DataFrame de carreras.
            fact_qualifying (pd.DataFrame): DataFrame de clasificación.
        """
        print("\n--- Data Loading Process Started ---")
        
        # 1. Borra y recrea la estructura de la base de datos
        from models import create_all_tables
        create_all_tables(self.engine)
        
        # 2. Carga las tablas de dimensión primero
        try:
            print("Loading dimension tables...")
            dim_drivers.to_sql('dim_drivers', con=self.engine, if_exists='append', index=False)
            dim_constructors.to_sql('dim_constructors', con=self.engine, if_exists='append', index=False)
            dim_circuits.to_sql('dim_circuits', con=self.engine, if_exists='append', index=False)
            dim_races.to_sql('dim_races', con=self.engine, if_exists='append', index=False)
            print("Dimension tables loaded successfully.")

            # 3. Carga la tabla de hechos
            print("Loading fact table...")
            fact_qualifying.to_sql('fact_qualifying', con=self.engine, if_exists='append', index=False, chunksize=10000)
            print("Fact table loaded successfully.")
            
        except Exception as e:
            print(f"An error occurred during data loading: {e}")
            raise
            
        print("\n--- Data Loading Process Finished Successfully! ---")