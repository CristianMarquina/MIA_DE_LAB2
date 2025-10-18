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
            self.df_status_raw = pd.read_csv(f"{self.data_path}/status.csv", na_values=['\\N'])
            # For facts
            self.df_qualifying_raw = pd.read_csv(f"{self.data_path}/qualifying.csv")
            self.df_pit_stops_raw = pd.read_csv(f"{self.data_path}/pit_stops.csv", na_values=['\\N'])
            self.df_results_raw = pd.read_csv(f"{self.data_path}/results.csv", na_values=['\\N'])
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
        # Data Cleaning: Drop rows with null IDs and remove duplicates
        df.dropna(subset=['driverId'], inplace=True)
        df.drop_duplicates(subset=['driverId'], inplace=True)

        df['full_name'] = df['forename'] + ' ' + df['surname']
        df = df[['driverRef', 'full_name', 'dob', 'nationality', 'driverId']] 
        df.insert(0, 'driver_id', range(1, len(df) + 1))
        df = df.rename(columns={'driverRef': 'driver_ref'})

        self.driver_id_map = pd.Series(df.driver_id.values, index=df.driverId).to_dict()

        df = df.drop(columns=['driverId'])  
        print("Processed number of records:", len(df))
        return df

    def process_dim_constructors(self):
        """Processes constructors data, cleans it, and adds an 'Unknown' record."""
        print("Processing Dimension: Constructors...")
        df = self.df_constructors_raw.copy()

        df.dropna(subset=['constructorId'], inplace=True)
        df.drop_duplicates(subset=['constructorId'], inplace=True)

        df = df[['constructorRef', 'name', 'nationality', 'constructorId']]  
        df.insert(0, 'constructor_id', range(1, len(df) + 1))
        df = df.rename(columns={'constructorRef': 'constructor_ref'})

        self.constructor_id_map = pd.Series(df.constructor_id.values, index=df.constructorId).to_dict()

        df = df.drop(columns=['constructorId'])  
        print("Processed number of records:", len(df))
        return df

    def process_dim_circuits(self):
        """Processes circuits data, cleans it, and adds an 'Unknown' record."""
        print("Processing Dimension: Circuits...")
        df = self.df_circuits_raw.copy()

        df.dropna(subset=['circuitId'], inplace=True)
        df.drop_duplicates(subset=['circuitId'], inplace=True)

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
        df = df[['year', 'round', 'circuitId', 'name', 'date', 'time', 'raceId']]  # Keep raceId temporarily
        df.insert(0, 'race_id', range(1, len(df) + 1))
        df = df.rename(columns={'circuitId': 'circuit_id'})

        # Create mapping dictionary
        self.race_id_map = pd.Series(df.race_id.values, index=df.raceId).to_dict()

        df = df.drop(columns=['raceId'])  # Remove original raceId
        return df

    def process_fact_qualifying(self):
        """
        Processes qualifying data, converting times to milliseconds and
        mapping foreign keys from the processed dimension tables.
        """
        print("Processing Fact Table: Qualifying...")
        df = self.df_qualifying_raw.copy()

        # Use mapping dictionaries to convert original IDs to new IDs
        df['race_id'] = df['raceId'].map(self.race_id_map)
        df['driver_id'] = df['driverId'].map(self.driver_id_map)
        df['constructor_id'] = df['constructorId'].map(self.constructor_id_map)

        df.dropna(subset=['driver_id', 'constructor_id'], inplace=True)
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
    
    def process_dim_status(self):
        """Procesa los datos de status para crear la dimensión DimStatus."""
        print("Processing Dimension: Status...")
        df = self.df_status_raw.copy()
        
        # Limpieza de datos
        df.dropna(subset=['statusId'], inplace=True)
        df.drop_duplicates(subset=['statusId'], inplace=True)
        
        # Renombrado de columnas y creación del mapa de traducción
        df = df.rename(columns={'statusId': 'status_id', 'status': 'status_description'})
        self.status_id_map = pd.Series(df.status_id.values, index=df.status_id).to_dict()
        
        return df
    
    # et.py - Añade esta nueva función a tu clase

    def process_fact_pit_stops(self):
        """
        Procesa los datos de pit stops, los enriquece con el constructorId
        y mapea las claves foráneas a los nuevos IDs secuenciales.
        """
        print("Processing Fact Table: Pit Stops...")
        df = self.df_pit_stops_raw.copy()

        # --- Paso 1: Enriquecimiento de Datos ---
        # Usamos results.csv para encontrar el constructorId para cada parada.
        # Seleccionamos solo las columnas necesarias y eliminamos duplicados para crear una tabla de consulta limpia.
        constructor_lookup = self.df_results_raw[['raceId', 'driverId', 'constructorId']].drop_duplicates()
        
        # Unimos (merge) los datos de pit stops con la tabla de consulta.
        df = pd.merge(df, constructor_lookup, on=['raceId', 'driverId'], how='left')

        # --- Paso 2: Mapeo de IDs ---
        # Usamos los mapas de traducción creados en los métodos de dimensión.
        df['race_id'] = df['raceId'].map(self.race_id_map)
        df['driver_id'] = df['driverId'].map(self.driver_id_map)
        df['constructor_id'] = df['constructorId'].map(self.constructor_id_map)

        # --- Paso 3: Limpieza de Datos ---
        # Eliminamos cualquier parada que no se pudo mapear a una carrera, piloto o constructor válido.
        fk_columns = ['race_id', 'driver_id', 'constructor_id']
        df.dropna(subset=fk_columns, inplace=True)
        df[fk_columns] = df[fk_columns].astype(int)

        # --- Paso 4: Selección Final de Columnas y Renombrado ---
        df = df.rename(columns={'stop': 'stop_number', 'milliseconds': 'duration_ms'})
        
        # Generamos un ID único para cada parada en boxes
        df.insert(0, 'pit_stop_id', range(1, len(df) + 1))
        
        final_columns = [
            'pit_stop_id', 'race_id', 'driver_id', 'constructor_id',
            'lap', 'stop_number', 'duration_ms'
        ]
        df = df[final_columns]
        
        return df
    
    # et.py - Nuevo método añadido

    def process_fact_race_results(self):
        """
        Procesa los datos de resultados de carrera, mapea las claves foráneas,
        calcula métricas derivadas y selecciona los hechos relevantes.
        """
        print("Processing Fact Table: Race Results...")
        df = self.df_results_raw.copy()

        # --- 1. Mapeo de IDs (Traducción) ---
        df['race_id'] = df['raceId'].map(self.race_id_map)
        df['driver_id'] = df['driverId'].map(self.driver_id_map)
        df['constructor_id'] = df['constructorId'].map(self.constructor_id_map)
        # El statusId ya es la clave correcta, no necesita mapa.
        df['status_id'] = df['statusId']

        # --- 2. Limpieza de Datos ---
        fk_columns = ['race_id', 'driver_id', 'constructor_id', 'status_id']
        df.dropna(subset=fk_columns, inplace=True)
        df[fk_columns] = df[fk_columns].astype(int)

        # --- 3. Cálculo de Métrica Derivada ---
        # Convertimos 'grid' y 'position' a numérico para poder calcular.
        # 'coerce' convierte los no-números en NaN.
        df['grid'] = pd.to_numeric(df['grid'], errors='coerce')
        df['position'] = pd.to_numeric(df['position'], errors='coerce')
        # Calculamos las posiciones ganadas. Si falta algún dato, el resultado será NaN.
        df['positions_gained'] = df['grid'] - df['position']

        # --- 4. Transformación de Hechos y Selección Final ---
        df['fastest_lap_time_ms'] = df['fastestLapTime'].apply(self._time_to_milliseconds)
        
        # Limpiamos y rellenamos nulos en las métricas numéricas con 0.
        numeric_facts = ['points', 'laps', 'fastestLap', 'rank', 'fastestLapSpeed', 'milliseconds', 'positions_gained']
        for col in numeric_facts:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # Renombramos las columnas
        df = df.rename(columns={
            'resultId': 'result_id',
            'fastestLap': 'fastest_lap',
            'fastestLapSpeed': 'fastest_lap_speed'
        })
        
        # Seleccionamos las columnas finales para la tabla de hechos.
        final_columns = [
            'result_id', 'race_id', 'driver_id', 'constructor_id', 'status_id',
            'position', 'grid', 'positions_gained', 'points', 'laps', 
            'milliseconds', 'fastest_lap', 'rank', 'fastest_lap_time_ms', 'fastest_lap_speed'
        ]
        df = df[final_columns]

        return df
        
        
