"""
Módulo para el procesamiento de datos en SageMaker.
Lee datos de entrada, limpia valores nulos y guarda el resultado.
"""
import os
import sys
import pandas as pd

INPUT_DATA_PATH = '/opt/ml/processing/input/data'
OUTPUT_DATA_PATH = '/opt/ml/processing/output/result'

def main():
    """
    Función principal que ejecuta el flujo de trabajo:
    1. Leer datos
    2. Procesar/Limpiar
    3. Guardar resultados
    """
    print("Iniciando trabajo de limpieza...", flush=True)

    try:
        if not os.path.exists(INPUT_DATA_PATH):
            print(f"Directorio de entrada {INPUT_DATA_PATH} no encontrado.", flush=True)
            parent = os.path.dirname(INPUT_DATA_PATH)
            if os.path.exists(parent):
                print(f"Contenido de {parent}: {os.listdir(parent)}", flush=True)
            raise FileNotFoundError(f"El directorio {INPUT_DATA_PATH} no existe.")

        print(f"Contenido de {INPUT_DATA_PATH}: {os.listdir(INPUT_DATA_PATH)}", flush=True)
        
        input_files = [f for f in os.listdir(INPUT_DATA_PATH) if f.lower().endswith('.csv')]
        if not input_files:
            raise FileNotFoundError(f"No se encontraron archivos CSV en {INPUT_DATA_PATH}. Archivos encontrados: {os.listdir(INPUT_DATA_PATH)}")

        input_filename = input_files[0]
        print(f"Leyendo archivo: {input_filename}", flush=True)

        df = pd.read_csv(os.path.join(INPUT_DATA_PATH, input_filename))

        print(f"Filas: {len(df)}", flush=True)

        if 'monto' in df.columns:
            df['igv'] = df['monto'] * 1.18

        if not os.path.exists(OUTPUT_DATA_PATH):
            os.makedirs(OUTPUT_DATA_PATH, exist_ok=True)

        output_file = os.path.join(OUTPUT_DATA_PATH, 'datos_procesados.csv')
        df.to_csv(output_file, index=False)

        print(f"Archivo guardado exitosamente en: {output_file}", flush=True)

    except Exception as e:
        error_msg = f"ERROR CRÍTICO EN PROCESAMIENTO: {e}"
        print(error_msg, flush=True)
        print(error_msg, file=sys.stderr)
        raise

if __name__ == "__main__":
    main()
