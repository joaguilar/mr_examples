# Ejemplos de MapReduce en Python

Seis ejemplos de jobs MapReduce sobre el dataset MovieLens.  
El framework [`mapreduce.py`](mapreduce.py) reemplaza `mrjob` con una implementación
pura en Python (sin dependencias externas) compatible con Python 3.10+.

## Requisitos

* Python 3.10 o superior (sin dependencias adicionales)

## Configuración del ambiente (opcional pero recomendado)

```bash
# Crear entorno virtual
python -m venv .venv

# Activar (Linux/macOS)
source .venv/bin/activate

# Activar (Windows)
.venv\Scripts\activate
```

## Datos

1. Descargar el dataset de [MovieLens](https://grouplens.org/datasets/movielens/)
2. Copiar los CSV al directorio `./data/`
3. Crear `ratings_noheader.csv` quitando la primera línea (encabezado) de `ratings.csv`:

```bash
# Linux/macOS
tail -n +2 data/ratings.csv > data/ratings_noheader.csv

# Windows PowerShell
Get-Content data\ratings.csv | Select-Object -Skip 1 | Set-Content data\ratings_noheader.csv
```

## Ejecución

Todos los scripts se ejecutan de la misma forma:

```bash
python <script>.py data/ratings_noheader.csv
```

### Ejemplos disponibles

| Script | Descripción |
|--------|-------------|
| `01_HistogramaCalificaciones.py` | Histograma de frecuencias de cada calificación |
| `02_PeliculasPorUsuario.py` | Cantidad de películas calificadas por usuario |
| `03_PeliculasPorUsuarioOrdenadas.py` | Ídem, ordenado de mayor a menor |
| `04_MasResenas.py` | Películas con más reseñas (ordenadas, clave fija) |
| `05_MasResenasPorPelicula.py` | Ídem, usando zero-padding como clave de sort |
| `06_MasResenasDistrib.py` | Ídem, usando clave negativa para sort distribuido |

### Ejemplo de salida

```bash
$ python 01_HistogramaCalificaciones.py data/ratings_noheader.csv
"0.5"   1101
"1.0"   3326
"1.5"   1687
"2.0"   7271
...
```

## Cómo funciona el framework

[`mapreduce.py`](mapreduce.py) implementa las tres fases de MapReduce localmente:

1. **Map** — el mapper procesa cada línea y emite pares `(clave, valor)`
2. **Shuffle & Sort** — los pares se ordenan por clave
3. **Reduce** — el reducer recibe cada clave con todos sus valores agrupados

Los jobs multi-step encadenan estos ciclos: la salida del reducer de un paso
es la entrada del mapper del siguiente.
