# Ejemplos de MapReduce usando mrjob

## Instalación usando Conda

### Requisitos

* Conda
* Python 3.9 

### Crear el ambiente

Creamos el ambiente usando conda, en este caso vamos a usar la version de python 3.12.2.

```
$ conda create --prefix ./.venv python=3.9.21
```

### Activar el ambiente e instalar dependencias

```
$ conda activate ./.venv
```

Dependencias:

```
pip install -f requirements.txt
```

## Datos

1. Obtener el dataset de movielens de [MovieLens](https://grouplens.org/datasets/movielens/)
2. Copiar los csvs al directorio ./data
3. Copiar el archivo ratings.csv a ratings_noheader.csv y elminar los encabezados