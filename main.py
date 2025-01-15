import boto3
import os

# Configura el cliente de S3
s3 = boto3.client('s3')

def download_bucket(bucket_name, download_path):
    """
    Descarga todos los archivos de un bucket S3 a una carpeta local,
    creando las carpetas necesarias.
    """
    if not os.path.exists(download_path):
        os.makedirs(download_path)

    objects = s3.list_objects_v2(Bucket=bucket_name)
    if 'Contents' not in objects:
        print(f"El bucket '{bucket_name}' no tiene objetos.")
        return 0

    count = 0
    for obj in objects['Contents']:
        key = obj['Key']
        local_file_path = os.path.join(download_path, key)

        # Crea directorios si no existen
        local_dir = os.path.dirname(local_file_path)
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)

        # Descarga el archivo
        s3.download_file(bucket_name, key, local_file_path)
        print(f"Descargado: {key}")
        count += 1

    print(f"\nTotal de objetos descargados: {count}")
    return count

def delete_objects(bucket_name):
    """
    Elimina todos los objetos de un bucket S3.
    """
    objects = s3.list_objects_v2(Bucket=bucket_name)
    if 'Contents' in objects:
        for obj in objects['Contents']:
            s3.delete_object(Bucket=bucket_name, Key=obj['Key'])
            print(f"Eliminado: {obj['Key']}")
        print("Todos los objetos han sido eliminados.")
    else:
        print("No hay objetos para eliminar.")

def delete_bucket(bucket_name):
    """
    Elimina un bucket de S3.
    """
    s3.delete_bucket(Bucket=bucket_name)
    print(f"El bucket '{bucket_name}' ha sido eliminado.")

def main():
    bucket_name = input("Ingresa el nombre del bucket: ")
    download_path = input("Ingresa la ruta de la carpeta donde descargar los archivos: ")

    print("\nDescargando objetos del bucket...")
    downloaded_count = download_bucket(bucket_name, download_path)

    if downloaded_count > 0:
        confirm_delete_objects = input("\n¿Deseas eliminar todos los objetos dentro del bucket? (y/n): ").strip().lower()
        if confirm_delete_objects == 'y':
            print("Eliminando objetos del bucket...")
            delete_objects(bucket_name)

    confirm_delete_bucket = input("\n¿Deseas eliminar el bucket? (y/n): ").strip().lower()
    if confirm_delete_bucket == 'y':
        print("Eliminando el bucket...")
        delete_bucket(bucket_name)

if __name__ == "__main__":
    main()
