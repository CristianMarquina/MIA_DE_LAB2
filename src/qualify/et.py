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
        print("Initial number of records:", len(df))
        print("Initial number of records:", len(df))
        print("Initial number of records:", len(df))
        print("Initial number of records:", len(df))
        print(len(df))
        # Data Cleaning: Drop rows with null IDs and remove duplicates
        df.dropna(subset=['driverId'], inplace=True)
        df.drop_duplicates(subset=['driverId'], inplace=True)
        
        df['full_name'] = df['forename'] + ' ' + df['surname']
        df = df[['driverRef', 'full_name', 'dob', 'nationality']]
        df.insert(0, 'driver_id', range(1, len(df) + 1))
        df = df.rename(columns={'driverRef': 'driver_ref'})
        print("Processed number of records:", len(df))
        print("Processed number of records:", len(df))
        print("Processed number of records:", len(df))
        return df

    def process_dim_constructors(self):
        """Processes constructors data, cleans it, and adds an 'Unknown' record."""
        print("Processing Dimension: Constructors...")
        df = self.df_constructors_raw.copy()
        
        df.dropna(subset=['constructorId'], inplace=True)
        df.drop_duplicates(subset=['constructorId'], inplace=True)
        
        df = df[['constructorRef', 'name', 'nationality']]
        df.insert(0, 'constructor_id', range(1, len(df) + 1))
        df = df.rename(columns={'constructorRef': 'constructor_ref'})
        return df

    def process_dim_circuits(self):
        """Processes circuits data, cleans it, and adds an 'Unknown' record."""
        print("Processing Dimension: Circuits...")
        df = self.df_circuits_raw.copy()
        
        df.dropna(subset=['circuitId'], inplace=True)
        df.drop_duplicates(subset=['circuitId'], inplace=True)
        
        #todo: preguntar si hay que generar un nuevo id debido a que ya exite una relacion el en dataset ocn los ids
        df = df[['circuitId', 'circuitRef', 'name', 'location', 'country']]
        df = df.rename(columns={'circuitId': 'circuit_id', 'circuitRef': 'circuit_ref'})
        return df

    def process_dim_races(self):
        """
        Processes races data, cleans it, and replaces null dates
        with a placeholder value ('1900-01-01').
        """
       
        print("Processing Dimension: Races...")
        df = self.df_races_raw.copy()
        # handle null dates by replacing them with a placeholder date
        placeholder_date = pd.to_datetime('1900-01-01')
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df['date'].fillna(placeholder_date)
        # delete null and duplicate raceId entries
        df.dropna(subset=['raceId'], inplace=True)
        df.drop_duplicates(subset=['raceId'], inplace=True)
        df = df[[ 'year', 'round', 'circuitId', 'name', 'date', 'time']]
        df.insert(0, 'race_id', range(1, len(df) + 1))
        df = df.rename(columns={'circuitId': 'circuit_id'})
        return df

    def process_fact_qualifying(self, dim_races, dim_drivers, dim_constructors):
        """
        Processes qualifying data, converting times to milliseconds and
        mapping foreign keys from the processed dimension tables.
        """
        print("Processing Fact Table: Qualifying...")
        df = self.df_qualifying_raw.copy()
        
        race_id_map = pd.Series(dim_races.race_id.values, index=dim_races.race_id).to_dict()
        driver_id_map = pd.Series(dim_drivers.driver_id.values, index=dim_drivers.driver_id).to_dict()
        constructor_id_map = pd.Series(dim_constructors.constructor_id.values, index=dim_constructors.constructor_id).to_dict()

        df['race_id'] = df['raceId'].map(race_id_map)
        df['driver_id'] = df['driverId'].map(driver_id_map)
        df['constructor_id'] = df['constructorId'].map(constructor_id_map)
        df.dropna(subset=['driver_id','constructor_id'], inplace=True)
        df[['race_id', 'driver_id', 'constructor_id']] = df[['race_id', 'driver_id', 'constructor_id']].astype(int)
        
        # --- Fact Transformation ---
        df['q1_time_ms'] = df['q1'].apply(self._time_to_milliseconds)
        df['q2_time_ms'] = df['q2'].apply(self._time_to_milliseconds)
        df['q3_time_ms'] = df['q3'].apply(self._time_to_milliseconds)

        time_cols = ['q1_time_ms', 'q2_time_ms', 'q3_time_ms']
        for col in time_cols:
            df[col] = df[col].fillna(0).astype(int)
        # Final column selection and renaming
        df = df.rename(columns={'qualifyId': 'qualify_id'})
        final_columns = [
            'qualify_id', 'race_id', 'driver_id', 'constructor_id', 
            'position', 'q1_time_ms', 'q2_time_ms', 'q3_time_ms'
        ]
        df = df[final_columns]
        
        return df