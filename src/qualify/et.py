import pandas as pd

class F1ETQualifyProcessor:
    """
    A class to handle the Extraction and Transformation of F1 data,
    including data cleaning and validation.
    """
    def __init__(self, data_path='.'):
        """
        Initializes the processor by loading all necessary raw data into memory.
        """
        print("Initializing F1ETLProcessor: Loading raw data...")
        self.data_path = data_path
        try:
            # For dimensions
            self.df_drivers_raw = pd.read_csv(f"{self.data_path}/drivers.csv")
            self.df_constructors_raw = pd.read_csv(f"{self.data_path}/constructors.csv")
            self.df_circuits_raw = pd.read_csv(f"{self.data_path}/circuits.csv")
            self.df_races_raw = pd.read_csv(f"{self.data_path}/races.csv")
            # For facts
            self.df_qualifying_raw = pd.read_csv(f"{self.data_path}/qualifying.csv")
            print("All raw data loaded successfully.")
        except FileNotFoundError as e:
            print(f"Error loading data: {e}. Make sure CSV files are in the '{self.data_path}' directory.")
            raise

    def _time_to_milliseconds(self, time_str):
        """Helper function to convert a time string 'M:SS.ms' to milliseconds."""
        if pd.isna(time_str) or time_str == '\\N':
            return None
        try:
            minutes, seconds = time_str.split(':')
            total_seconds = int(minutes) * 60 + float(seconds)
            return int(total_seconds * 1000)
        except (ValueError, AttributeError):
            return None

    def process_dim_drivers(self):
        """Processes drivers data, cleans it, and adds an 'Unknown' record."""
        print("Processing Dimension: Drivers...")
        df = self.df_drivers_raw.copy()
        
        # Data Cleaning: Drop rows with null IDs and remove duplicates
        df.dropna(subset=['driverId'], inplace=True)
        df.drop_duplicates(subset=['driverId'], inplace=True)
        
        df['full_name'] = df['forename'] + ' ' + df['surname']
        df = df[['driverId', 'driverRef', 'full_name', 'dob', 'nationality']]
        df = df.rename(columns={'driverId': 'driver_id', 'driverRef': 'driver_ref'})
        
        # Add 'Unknown' record with ID 0 for referential integrity
        unknown_driver = pd.DataFrame([{"driver_id": 0, "driver_ref": "unknown", "full_name": "Unknown Driver"}])
        df = pd.concat([unknown_driver, df], ignore_index=True)
        
        return df

    def process_dim_constructors(self):
        """Processes constructors data, cleans it, and adds an 'Unknown' record."""
        print("Processing Dimension: Constructors...")
        df = self.df_constructors_raw.copy()
        
        df.dropna(subset=['constructorId'], inplace=True)
        df.drop_duplicates(subset=['constructorId'], inplace=True)
        
        df = df[['constructorId', 'constructorRef', 'name', 'nationality']]
        df = df.rename(columns={'constructorId': 'constructor_id', 'constructorRef': 'constructor_ref'})
        
        unknown_constructor = pd.DataFrame([{"constructor_id": 0, "constructor_ref": "unknown", "name": "Unknown Constructor"}])
        df = pd.concat([unknown_constructor, df], ignore_index=True)
        
        return df

    def process_dim_circuits(self):
        """Processes circuits data, cleans it, and adds an 'Unknown' record."""
        print("Processing Dimension: Circuits...")
        df = self.df_circuits_raw.copy()
        
        df.dropna(subset=['circuitId'], inplace=True)
        df.drop_duplicates(subset=['circuitId'], inplace=True)
        
        df = df[['circuitId', 'circuitRef', 'name', 'location', 'country']]
        df = df.rename(columns={'circuitId': 'circuit_id', 'circuitRef': 'circuit_ref'})
        
        unknown_circuit = pd.DataFrame([{"circuit_id": 0, "circuit_ref": "unknown", "name": "Unknown Circuit"}])
        df = pd.concat([unknown_circuit, df], ignore_index=True)
        
        return df

    def process_dim_races(self):
        """
        Processes races data, cleans it, and replaces null dates
        with a placeholder value ('1900-01-01').
        """
       
        print("Processing Dimension: Races...")
        df = self.df_races_raw.copy()
        
        # --- LÓGICA DE MANEJO DE NULOS EN FECHAS ---
        
        # 1. Define la fecha centinela que usaremos como reemplazo
        placeholder_date = pd.to_datetime('1900-01-01')
        
        # 2. Convierte la columna 'date' a formato datetime.
        #    'errors=coerce' convierte los valores que no puede parsear (incluyendo los NaN) en NaT (Not a Time).
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        
        # 3. Reemplaza todos los valores nulos (NaT) con nuestra fecha centinela.
        df['date'].fillna(placeholder_date)
        
        # --- FIN DE LA LÓGICA DE MANEJO DE NULOS ---
        
        df.dropna(subset=['raceId'], inplace=True)
        df.drop_duplicates(subset=['raceId'], inplace=True)
        
        df = df[['raceId', 'year', 'round', 'circuitId', 'name', 'date', 'time']]
        df = df.rename(columns={'raceId': 'race_id', 'circuitId': 'circuit_id'})
        
        return df

    def process_fact_qualifying(self, dim_races, dim_drivers, dim_constructors):
        """
        Processes qualifying data, converting times to milliseconds and
        mapping foreign keys from the processed dimension tables.
        """
        print("Processing Fact Table: Qualifying...")
        df = self.df_qualifying_raw.copy()
        
        # --- Foreign Key Mapping (CORRECTED AND MORE ROBUST LOGIC) ---

        # 1. Create mapping dictionaries. This is a more explicit and safer way.
        #    We tell Pandas: use the original ID column as the key and the final ID column as the value.
        print("1. Creating mapping dictionaries for foreign keys...")
        race_id_map = dim_races.set_index('race_id')['race_id'].to_dict()
        driver_id_map = dim_drivers.set_index('driver_id')['driver_id'].to_dict()
        constructor_id_map = dim_constructors.set_index('constructor_id')['constructor_id'].to_dict()
        print("2. Mapping dictionaries created.")

        # 2. Apply the maps to the ORIGINAL columns from the raw qualifying data.
        df['race_id'] = df['raceId'].map(race_id_map)
        df['driver_id'] = df['driverId'].map(driver_id_map)
        df['constructor_id'] = df['constructorId'].map(constructor_id_map)
        print("3. Foreign keys mapped using the original IDs.")
        
        # --- End of Correction ---
        print("4. Foreign key mapping completed.")
        # Fill any driver or constructor IDs that weren't found with 0 (for the 'Unknown' record)
        df['driver_id'].fillna(0, inplace=True)
        df['constructor_id'].fillna(0, inplace=True)
        print("5. Missing foreign keys filled with 'Unknown' record ID (0).")
        # Data Integrity: A qualifying result MUST belong to a valid race.
        # Drop any rows where the race_id could not be mapped.
        df.dropna(subset=['race_id'], inplace=True)
        print("6. Rows with invalid race.")
        # Convert foreign key columns to integer type.
        df[['race_id', 'driver_id', 'constructor_id']] = df[['race_id', 'driver_id', 'constructor_id']].astype(int)
        
        # --- Fact Transformation ---
        df['q1_time_ms'] = df['q1'].apply(self._time_to_milliseconds)
        df['q2_time_ms'] = df['q2'].apply(self._time_to_milliseconds)
        df['q3_time_ms'] = df['q3'].apply(self._time_to_milliseconds)
        print("7. Qualifying times converted to milliseconds.")
        # Final column selection and renaming
        df = df.rename(columns={'qualifyId': 'qualify_id', 'number': 'car_number'})
        final_columns = [
            'qualify_id', 'race_id', 'driver_id', 'constructor_id', 
            'car_number', 'position', 'q1_time_ms', 'q2_time_ms', 'q3_time_ms'
        ]
        print("8. Final column selection completed.")
        df = df[final_columns]
        
        return df